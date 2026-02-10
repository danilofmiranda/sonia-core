"""
SonIA Core - Daily Tracking Orchestrator
BloomsPal / Fase 1

Runs daily at 4:00 AM COT (UTC-5):
1. Read shipments from DynamoDB
2. Group by tenant, look up each in Odoo
3. Query FedEx Track API for status updates
4. Store/update in PostgreSQL
5. Detect anomalies and create proactive claims
6. Generate per-client reports
7. Send reports via WhatsApp (through SonIA Agent)

Error handling: ALL errors are notified to admin via WhatsApp.
"""

import os
import logging
import traceback
import psycopg2
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from typing import Dict, List, Any, Optional
from collections import defaultdict

from fastapi import FastAPI, HTTPException
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from modules.dynamo_reader import DynamoReader
from modules.fedex_tracker import FedExTracker
from modules.db_manager import DBManager
from modules.anomaly_detector import AnomalyDetector
from modules.report_generator import ReportGenerator
from modules.whatsapp_sender import WhatsAppSender
from modules.odoo_client import OdooClient

# ââ Logging ââ
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("sonia-core")

COT = timezone(timedelta(hours=-5))


# ============================================================================
# DATABASE MIGRATION
# ============================================================================

def run_migration():
    """Auto-run SQL migrations on startup."""
    db_url = os.getenv("DATABASE_URL", "")
    if not db_url:
        logger.warning("No DATABASE_URL - skipping migration")
        return

    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        cur = conn.cursor()

        migration_file = os.path.join(os.path.dirname(__file__), "migrations", "001_initial_schema.sql")
        if os.path.exists(migration_file):
            with open(migration_file, "r") as f:
                sql = f.read()
            cur.execute(sql)
            logger.info("Migration 001 applied successfully")
        else:
            logger.warning(f"Migration file not found: {migration_file}")

        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Migration error: {e}")


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    DATABASE_URL = os.getenv("DATABASE_URL", "")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
    DYNAMO_TABLE = os.getenv("DYNAMO_TABLE_RESERVES", "reserves")
    FEDEX_API_KEY = os.getenv("FEDEX_API_KEY", "")
    FEDEX_SECRET_KEY = os.getenv("FEDEX_SECRET_KEY", "")
    FEDEX_ACCOUNT = os.getenv("FEDEX_ACCOUNT", "")
    ODOO_URL = os.getenv("ODOO_URL", "")
    ODOO_DB = os.getenv("ODOO_DB", "")
    ODOO_USER = os.getenv("ODOO_USER", "")
    ODOO_PASSWORD = os.getenv("ODOO_PASSWORD", "")
    ODOO_TENANT_FIELD = os.getenv("ODOO_TENANT_FIELD", "x_studio_tenant")
    SONIA_AGENT_URL = os.getenv("SONIA_AGENT_URL", "")
    SONIA_AGENT_API_KEY = os.getenv("SONIA_AGENT_API_KEY", "")
    ADMIN_WHATSAPP = os.getenv("ADMIN_WHATSAPP", "")
    RUN_HOUR_COT = int(os.getenv("RUN_HOUR_COT", "4"))
    ENVIRONMENT = os.getenv("ENVIRONMENT", "development")


config = Config()


# ============================================================================
# MODULE INITIALIZATION
# ============================================================================

def init_modules():
    """Initialize all modules with config."""
    mods = {}

    # Database
    if config.DATABASE_URL:
        mods["db"] = DBManager(config.DATABASE_URL)
        logger.info("DBManager initialized")
    else:
        logger.warning("DATABASE_URL not set - DB features disabled")

    # DynamoDB Reader
    if config.AWS_ACCESS_KEY_ID:
        mods["dynamo"] = DynamoReader(
            aws_access_key=config.AWS_ACCESS_KEY_ID,
            aws_secret_key=config.AWS_SECRET_ACCESS_KEY,
            region=config.AWS_REGION,
            table_name=config.DYNAMO_TABLE,
        )
        logger.info("DynamoReader initialized")

    # FedEx Tracker
    if config.FEDEX_API_KEY:
        mods["fedex"] = FedExTracker(
            client_id=config.FEDEX_API_KEY,
            client_secret=config.FEDEX_SECRET_KEY,
            account_number=config.FEDEX_ACCOUNT,
        )
        logger.info("FedExTracker initialized")

    # Anomaly Detector
    mods["anomaly"] = AnomalyDetector()
    logger.info("AnomalyDetector initialized")

    # Report Generator
    mods["reports"] = ReportGenerator()
    logger.info("ReportGenerator initialized")

    # WhatsApp Sender
    if config.SONIA_AGENT_URL:
        mods["whatsapp"] = WhatsAppSender(
            agent_url=config.SONIA_AGENT_URL,
            api_key=config.SONIA_AGENT_API_KEY,
        )
        logger.info("WhatsAppSender initialized")

    # Odoo Client
    if config.ODOO_URL and config.ODOO_USER:
        mods["odoo"] = OdooClient(
            url=config.ODOO_URL,
            db=config.ODOO_DB,
            username=config.ODOO_USER,
            password=config.ODOO_PASSWORD,
        )
        logger.info("OdooClient initialized")

    return mods


# ============================================================================
# ADMIN WHATSAPP ALERT HELPERS
# ============================================================================

def _send_admin_alert(whatsapp: WhatsAppSender, message: str):
    """Send a WhatsApp alert to the admin. Never fails silently."""
    if not whatsapp or not config.ADMIN_WHATSAPP:
        logger.error(f"Cannot send admin alert (no whatsapp/admin number): {message}")
        return
    try:
        whatsapp.send_alert_sync(config.ADMIN_WHATSAPP, message)
        logger.info("Admin alert sent via WhatsApp")
    except Exception as e:
        logger.error(f"CRITICAL: Failed to send admin alert: {e}. Message was: {message}")


def _alert_tenant_not_found(whatsapp: WhatsAppSender, tenant_number: int,
                             tracking_numbers: List[str]):
    """Alert admin: tenant exists in DynamoDB but not in Odoo."""
    now = datetime.now(COT).strftime("%d/%m/%Y %I:%M %p")
    guides = ", ".join(tracking_numbers[:5])
    if len(tracking_numbers) > 5:
        guides += f" ... (+{len(tracking_numbers) - 5} mas)"

    msg = (
        f"\u26a0\ufe0f *SonIA Tracker \u2014 Alerta*\n\n"
        f"Tenant #{tenant_number} existe en DynamoDB pero no se "
        f"encontro en Odoo.\n\n"
        f"\U0001f4e6 Guias pendientes: {len(tracking_numbers)}\n"
        f"\U0001f4cb Tracking: {guides}\n\n"
        f'*Accion requerida:* Verificar que el campo "Tenant #" '
        f"esta configurado correctamente en Odoo para este tenant.\n\n"
        f"\U0001f916 SonIA Tracker \u2014 {now}"
    )
    _send_admin_alert(whatsapp, msg)


def _alert_no_whatsapp_contacts(whatsapp: WhatsAppSender, company_name: str,
                                 tenant_number: int, tracking_count: int):
    """Alert admin: company has no WhatsApp contacts in Odoo."""
    now = datetime.now(COT).strftime("%d/%m/%Y %I:%M %p")
    msg = (
        f"\u26a0\ufe0f *SonIA Tracker \u2014 Alerta*\n\n"
        f"La compania {company_name} (Tenant #{tenant_number}) "
        f"no tiene usuarios con WhatsApp asignados en Odoo.\n\n"
        f"\U0001f4e6 Guias pendientes: {tracking_count}\n\n"
        f"*Accion requerida:* Agregar contactos con numero de "
        f"WhatsApp a esta compania en Odoo.\n\n"
        f"\U0001f916 SonIA Tracker \u2014 {now}"
    )
    _send_admin_alert(whatsapp, msg)


def _alert_flow_error(whatsapp: WhatsAppSender, tenant_number: Optional[int],
                       client_name: str, error_type: str, error_msg: str,
                       affected_count: int, total_count: int,
                       scope: str = "Solo este cliente"):
    """Alert admin: error during SonIA Tracker processing."""
    now = datetime.now(COT).strftime("%d/%m/%Y %I:%M %p")
    client_label = f"{client_name} (Tenant #{tenant_number})" if tenant_number else client_name
    msg = (
        f"\U0001f6a8 *SonIA Tracker \u2014 Error*\n\n"
        f"Se produjo un error durante el ciclo diario:\n\n"
        f"\U0001f464 Cliente/Tenant: {client_label}\n"
        f"\u274c Tipo de error: {error_type}\n"
        f"\U0001f4e6 Guias afectadas: {affected_count}\n"
        f"\U0001f4ca Total guias en ciclo: {total_count} (excluyendo entregadas)\n"
        f"\U0001f504 Alcance: {scope}\n\n"
        f"Detalle: {error_msg[:300]}\n\n"
        f"\U0001f916 SonIA Tracker \u2014 {now}"
    )
    _send_admin_alert(whatsapp, msg)


# ============================================================================
# DAILY ORCHESTRATION FLOW
# ============================================================================

async def daily_flow(modules: dict):
    """
    Main daily orchestration with Odoo-based tenant mapping:
    1. Read shipments from DynamoDB
    2. Group by tenant number
    3. For each tenant: Odoo lookup -> FedEx tracking -> report -> WhatsApp
    4. ALL errors are notified to admin via WhatsApp
    """
    db = modules.get("db")
    dynamo = modules.get("dynamo")
    fedex = modules.get("fedex")
    anomaly_detector = modules.get("anomaly")
    report_gen = modules.get("reports")
    whatsapp = modules.get("whatsapp")
    odoo = modules.get("odoo")

    if not db:
        logger.error("DB not available, cannot run daily flow")
        return

    now = datetime.now(COT)
    logger.info(f"=== Starting daily flow at {now.strftime('%Y-%m-%d %H:%M:%S')} COT ===")

    # Create run log
    run_id = db.create_run_log(now.date())
    stats = {
        "total_shipments_read": 0,
        "tenants_found": 0,
        "tenants_in_odoo": 0,
        "tenants_missing_odoo": 0,
        "tenants_no_whatsapp": 0,
        "new_shipments": 0,
        "shipments_checked": 0,
        "shipments_updated": 0,
        "shipments_delivered": 0,
        "claims_created": 0,
        "reports_generated": 0,
        "reports_sent": 0,
        "alerts_sent": 0,
    }
    errors = []
    total_active_packages = 0  # Total non-delivered packages across all tenants

    try:
        # ââ Step 1: Read from DynamoDB ââ
        logger.info("Step 1: Reading shipments from DynamoDB...")
        raw_shipments = []
        if dynamo:
            try:
                raw_shipments = dynamo.scan_all_shipments()
                stats["total_shipments_read"] = len(raw_shipments)
                logger.info(f"Read {len(raw_shipments)} reserves from DynamoDB")
            except Exception as e:
                logger.error(f"DynamoDB read error: {e}")
                errors.append({"step": "dynamo_read", "error": str(e)})
                _alert_flow_error(
                    whatsapp, None, "N/A", "Error leyendo DynamoDB",
                    str(e), 0, 0, "Todo el flujo diario se detuvo"
                )
                stats["alerts_sent"] += 1
                db.update_run_log(run_id, stats, errors, "failed")
                return
        else:
            logger.error("DynamoDB module not available")
            _alert_flow_error(
                whatsapp, None, "N/A", "Modulo DynamoDB no disponible",
                "DynamoReader no inicializado", 0, 0,
                "Todo el flujo diario se detuvo"
            )
            stats["alerts_sent"] += 1
            db.update_run_log(run_id, stats, errors, "failed")
            return

        if not raw_shipments:
            logger.info("No shipments found in DynamoDB. Nothing to process.")
            db.update_run_log(run_id, stats, errors, "success")
            return

        # ââ Step 2: Group by tenant ââ
        logger.info("Step 2: Grouping shipments by tenant...")
        tenant_groups = defaultdict(list)
        for reserve in raw_shipments:
            tenant_id = reserve.get("tenant")
            if tenant_id is not None:
                tenant_groups[int(tenant_id)].append(reserve)
            else:
                logger.warning(f"Reserve {reserve.get('id', '?')} has no tenant ID")

        stats["tenants_found"] = len(tenant_groups)
        logger.info(f"Found {len(tenant_groups)} unique tenants: {list(tenant_groups.keys())}")

        # Count total active (non-delivered) packages across all tenants
        for tenant_id, reserves in tenant_groups.items():
            for reserve in reserves:
                for pkg in reserve.get("packages", []):
                    if pkg.get("status", "").lower() != "delivered":
                        total_active_packages += 1

        # ââ Step 3: Authenticate with Odoo ââ
        logger.info("Step 3: Authenticating with Odoo...")
        odoo_ok = False
        if odoo:
            try:
                odoo_ok = odoo.authenticate()
                if not odoo_ok:
                    raise RuntimeError("Odoo authentication returned False")
            except Exception as e:
                logger.error(f"Odoo authentication error: {e}")
                errors.append({"step": "odoo_auth", "error": str(e)})
                _alert_flow_error(
                    whatsapp, None, "N/A", "Error autenticacion Odoo",
                    str(e), total_active_packages, total_active_packages,
                    "Todo el flujo diario se detuvo"
                )
                stats["alerts_sent"] += 1
                db.update_run_log(run_id, stats, errors, "failed")
                return
        else:
            logger.error("Odoo module not available")
            _alert_flow_error(
                whatsapp, None, "N/A", "Modulo Odoo no disponible",
                "OdooClient no inicializado. Verificar variables ODOO_*",
                total_active_packages, total_active_packages,
                "Todo el flujo diario se detuvo"
            )
            stats["alerts_sent"] += 1
            db.update_run_log(run_id, stats, errors, "failed")
            return

        # ââ Step 4: Process each tenant ââ
        logger.info("Step 4: Processing tenants...")
        for tenant_id, reserves in tenant_groups.items():
            try:
                await _process_tenant(
                    tenant_id=tenant_id,
                    reserves=reserves,
                    modules=modules,
                    stats=stats,
                    errors=errors,
                    total_active_packages=total_active_packages,
                )
            except Exception as e:
                logger.error(f"Critical error processing tenant #{tenant_id}: {e}")
                tb = traceback.format_exc()
                logger.error(tb)
                errors.append({
                    "step": f"process_tenant_{tenant_id}",
                    "error": str(e),
                    "traceback": tb[:500],
                })

                # Count affected packages for this tenant
                tenant_pkgs = sum(
                    len(r.get("packages", [])) for r in reserves
                )
                _alert_flow_error(
                    whatsapp, tenant_id, f"Tenant #{tenant_id}",
                    "Error critico procesando tenant",
                    str(e), tenant_pkgs, total_active_packages,
                    "Solo este cliente"
                )
                stats["alerts_sent"] += 1

        # ââ Finalize ââ
        status = "success" if not errors else "partial"
        db.update_run_log(run_id, stats, errors, status)
        logger.info(f"=== Daily flow completed: {status} | Stats: {stats} ===")

        # Send admin summary if there were errors
        if errors:
            now_str = datetime.now(COT).strftime("%d/%m/%Y %I:%M %p")
            summary = (
                f"\U0001f4ca *SonIA Tracker \u2014 Resumen Diario*\n\n"
                f"Estado: {'Parcial' if status == 'partial' else 'Fallido'}\n"
                f"Errores: {len(errors)}\n"
                f"Tenants procesados: {stats['tenants_in_odoo']}/{stats['tenants_found']}\n"
                f"Reportes enviados: {stats['reports_sent']}\n"
                f"Alertas enviadas: {stats['alerts_sent']}\n\n"
                f"\U0001f916 SonIA Tracker \u2014 {now_str}"
            )
            _send_admin_alert(whatsapp, summary)

    except Exception as e:
        logger.error(f"Daily flow critical error: {e}")
        tb = traceback.format_exc()
        logger.error(tb)
        if db and run_id:
            errors.append({"step": "critical", "error": str(e)})
            db.update_run_log(run_id, stats, errors, "failed")
        _alert_flow_error(
            whatsapp, None, "N/A", "Error critico en flujo diario",
            str(e), total_active_packages, total_active_packages,
            "Todo el flujo diario se detuvo"
        )


async def _process_tenant(tenant_id: int, reserves: List[Dict],
                           modules: dict, stats: dict, errors: list,
                           total_active_packages: int):
    """
    Process all reserves for a single tenant:
    1. Look up company in Odoo
    2. Get WhatsApp contacts
    3. Extract tracking numbers
    4. Store in PostgreSQL
    5. Query FedEx for active shipments
    6. Detect anomalies
    7. Generate and send report
    """
    db = modules.get("db")
    fedex = modules.get("fedex")
    anomaly_detector = modules.get("anomaly")
    report_gen = modules.get("reports")
    whatsapp = modules.get("whatsapp")
    odoo = modules.get("odoo")

    logger.info(f"--- Processing Tenant #{tenant_id} ({len(reserves)} reserves) ---")

    # Collect all tracking numbers from packages
    all_tracking = []
    active_tracking = []
    for reserve in reserves:
        for pkg in reserve.get("packages", []):
            tn = pkg.get("tracking_number", "")
            if tn:
                all_tracking.append(tn)
                if pkg.get("status", "").lower() != "delivered":
                    active_tracking.append(tn)

    logger.info(f"Tenant #{tenant_id}: {len(all_tracking)} total packages, {len(active_tracking)} active")

    # ââ 4a: Look up company in Odoo ââ
    company = odoo.find_company_by_tenant_number(
        tenant_id, field_name=config.ODOO_TENANT_FIELD
    )

    if not company:
        logger.warning(f"Tenant #{tenant_id} not found in Odoo!")
        _alert_tenant_not_found(whatsapp, tenant_id, active_tracking or all_tracking)
        stats["tenants_missing_odoo"] += 1
        stats["alerts_sent"] += 1

        # Still store shipments in PostgreSQL even without Odoo match
        for reserve in reserves:
            for pkg in reserve.get("packages", []):
                tn = pkg.get("tracking_number", "")
                if tn:
                    try:
                        db.upsert_shipment(
                            tracking_number=tn,
                            client_id=None,
                            client_name_raw=f"Tenant #{tenant_id} (sin Odoo)",
                            dynamo_data=reserve,
                        )
                    except Exception:
                        pass
        return

    company_name = company.get("name", f"Tenant #{tenant_id}")
    company_id = company["id"]
    stats["tenants_in_odoo"] += 1
    logger.info(f"Tenant #{tenant_id} -> Odoo company: {company_name} (ID: {company_id})")

    # ââ 4b: Get WhatsApp contacts ââ
    wa_contacts = odoo.get_whatsapp_contacts_for_company(company_id)

    if not wa_contacts:
        logger.warning(f"No WhatsApp contacts for {company_name} (Tenant #{tenant_id})")
        _alert_no_whatsapp_contacts(whatsapp, company_name, tenant_id, len(active_tracking))
        stats["tenants_no_whatsapp"] += 1
        stats["alerts_sent"] += 1
        # Continue processing (FedEx, anomalies) but won't send reports

    # ââ 4c: Store shipments in PostgreSQL ââ
    # Get or create client in DB
    client_info = db.get_client_by_tenant(tenant_id)
    client_db_id = client_info["id"] if client_info else None

    for reserve in reserves:
        for pkg in reserve.get("packages", []):
            tn = pkg.get("tracking_number", "")
            if not tn:
                continue
            try:
                inserted = db.upsert_shipment(
                    tracking_number=tn,
                    client_id=client_db_id,
                    client_name_raw=company_name,
                    dynamo_data=reserve,
                )
                if inserted:
                    stats["new_shipments"] += 1
            except Exception as e:
                logger.error(f"DB upsert error for {tn}: {e}")

    # ââ 4d: Query FedEx for active tracking numbers ââ
    if fedex and active_tracking:
        logger.info(f"Querying FedEx for {len(active_tracking)} active packages...")
        batch_size = 30
        for i in range(0, len(active_tracking), batch_size):
            batch = active_tracking[i:i + batch_size]
            try:
                results = fedex.track_multiple(batch)
                stats["shipments_checked"] += len(batch)

                for tracking_num, fedex_data in results.items():
                    if fedex_data.get("error"):
                        continue
                    updated = db.update_shipment_from_fedex(
                        tracking_number=tracking_num,
                        fedex_data=fedex_data,
                    )
                    if updated:
                        stats["shipments_updated"] += 1
                        if fedex_data.get("is_delivered"):
                            stats["shipments_delivered"] += 1
            except Exception as e:
                logger.error(f"FedEx batch error for tenant #{tenant_id}: {e}")
                errors.append({
                    "step": f"fedex_track_tenant_{tenant_id}",
                    "error": str(e),
                    "batch_index": i,
                })
                _alert_flow_error(
                    whatsapp, tenant_id, company_name,
                    "Error consultando FedEx",
                    str(e), len(batch), total_active_packages,
                    "Solo este cliente"
                )
                stats["alerts_sent"] += 1

    # ââ 4e: Detect anomalies ââ
    if anomaly_detector and client_db_id:
        try:
            client_shipments = db.get_shipments_by_client(client_db_id)
            if client_shipments:
                shipment_dicts = [dict(s) for s in client_shipments]
                anomalies = anomaly_detector.check_all_shipments(shipment_dicts)

                for anomaly in anomalies:
                    claim_id = db.create_proactive_claim(
                        tracking_number=anomaly["tracking_number"],
                        shipment_id=anomaly.get("shipment_id"),
                        client_id=client_db_id,
                        claim_type=anomaly["claim_type"],
                        description=anomaly["description"],
                        rule=anomaly["rule"],
                    )
                    if claim_id:
                        stats["claims_created"] += 1
        except Exception as e:
            logger.error(f"Anomaly detection error for tenant #{tenant_id}: {e}")
            errors.append({
                "step": f"anomaly_tenant_{tenant_id}",
                "error": str(e),
            })

    # ââ 4f: Generate report ââ
    if report_gen and client_db_id:
        try:
            client_shipments = db.get_shipments_by_client(client_db_id)
            if client_shipments:
                report = report_gen.generate_client_report(
                    client_name=company_name,
                    shipments=[dict(s) for s in client_shipments],
                )
                stats["reports_generated"] += 1

                # ââ 4g: Send report via WhatsApp ââ
                if whatsapp and wa_contacts:
                    for contact in wa_contacts:
                        try:
                            sent = whatsapp.send_report_sync(
                                phone_number=contact["whatsapp_number"],
                                report_text=report,
                                client_name=company_name,
                            )
                            if sent:
                                stats["reports_sent"] += 1
                                logger.info(
                                    f"Report sent to {contact['name']} "
                                    f"({contact['whatsapp_number']}) "
                                    f"for {company_name}"
                                )
                        except Exception as e:
                            logger.error(
                                f"WhatsApp send error to {contact['whatsapp_number']}: {e}"
                            )
                            errors.append({
                                "step": f"whatsapp_send_tenant_{tenant_id}",
                                "contact": contact["name"],
                                "error": str(e),
                            })
                            _alert_flow_error(
                                whatsapp, tenant_id, company_name,
                                "Error enviando reporte WhatsApp",
                                f"Contacto: {contact['name']} - {str(e)}",
                                len(active_tracking), total_active_packages,
                                "Solo este cliente"
                            )
                            stats["alerts_sent"] += 1
        except Exception as e:
            logger.error(f"Report generation error for tenant #{tenant_id}: {e}")
            errors.append({
                "step": f"report_gen_tenant_{tenant_id}",
                "error": str(e),
            })

    logger.info(f"--- Tenant #{tenant_id} ({company_name}) processing complete ---")


# ============================================================================
# FASTAPI APP
# ============================================================================

scheduler = AsyncIOScheduler()
modules = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown."""
    global modules

    # Run migration
    logger.info("Running database migration check...")
    run_migration()

    # Initialize modules
    logger.info("Initializing modules...")
    modules = init_modules()

    # Schedule daily job
    run_hour = config.RUN_HOUR_COT
    scheduler.add_job(
        daily_flow,
        CronTrigger(hour=run_hour, minute=0, timezone=COT),
        args=[modules],
        id="daily_flow",
        name=f"SonIA Daily Flow ({run_hour}:00 COT)",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"Scheduler started - daily flow at {run_hour}:00 COT")

    yield

    # Shutdown
    scheduler.shutdown()
    if "db" in modules:
        modules["db"].close()
    logger.info("SonIA Core shut down")


app = FastAPI(
    title="SonIA Core",
    description="Daily Tracking Orchestrator - BloomsPal",
    version="1.1.0",
    lifespan=lifespan,
)


# ============================================================================
# STANDARD ENDPOINTS
# ============================================================================

@app.get("/")
async def root():
    return {
        "service": "SonIA Core",
        "status": "running",
        "environment": config.ENVIRONMENT,
        "version": "1.1.0",
    }


@app.get("/health")
async def health():
    db = modules.get("db")
    db_ok = False
    if db:
        try:
            db_ok = db.health_check()
        except Exception:
            pass

    return {
        "status": "healthy" if db_ok else "degraded",
        "database": "connected" if db_ok else "disconnected",
        "modules": list(modules.keys()),
        "timestamp": datetime.now(COT).isoformat(),
    }


@app.post("/run")
async def trigger_run():
    """Manually trigger the daily flow."""
    if not modules:
        raise HTTPException(status_code=503, detail="Modules not initialized")

    import asyncio
    asyncio.create_task(daily_flow(modules))
    return {"message": "Daily flow triggered", "timestamp": datetime.now(COT).isoformat()}


@app.get("/stats")
async def get_stats():
    """Get latest run statistics."""
    db = modules.get("db")
    if not db:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        db.connect()
        db.cursor.execute(
            "SELECT * FROM daily_run_logs ORDER BY created_at DESC LIMIT 5"
        )
        runs = db.cursor.fetchall()
        db.close()
        return {"recent_runs": [dict(r) for r in runs] if runs else []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ADMIN ENDPOINTS
# ============================================================================

@app.get("/admin/dynamo-scan")
async def admin_dynamo_scan():
    """Scan DynamoDB to see sample records and identify tenant IDs."""
    dynamo = modules.get("dynamo")
    if not dynamo:
        raise HTTPException(status_code=503, detail="DynamoDB not available")

    try:
        response = dynamo.client.scan(
            TableName=dynamo.table_name,
            Limit=10
        )
        items = response.get("Items", [])
        return {
            "table": dynamo.table_name,
            "count": response.get("Count", 0),
            "scanned": response.get("ScannedCount", 0),
            "items": items
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/db-status")
async def admin_db_status():
    """Check database tables and record counts."""
    db = modules.get("db")
    if not db:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        db.connect()
        tables = ['clients', 'tenant_mapping', 'client_contacts',
                   'shipments', 'claims', 'daily_run_logs']
        counts = {}
        for table in tables:
            db.cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            result = db.cursor.fetchone()
            counts[table] = result["count"] if result else 0

        db.cursor.execute("SELECT * FROM tenant_mapping")
        mappings = db.cursor.fetchall()

        db.close()
        return {
            "table_counts": counts,
            "tenant_mappings": [dict(m) for m in mappings] if mappings else []
        }
    except Exception as e:
        db.close()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/test-whatsapp")
async def admin_test_whatsapp(phone: str = None, message: str = None):
    """
    Test WhatsApp message delivery via SonIA Agent.
    Defaults to sending a test message to the admin number.
    """
    whatsapp = modules.get("whatsapp")
    if not whatsapp:
        raise HTTPException(status_code=503, detail="WhatsApp module not available")

    target_phone = phone or config.ADMIN_WHATSAPP
    if not target_phone:
        raise HTTPException(status_code=400, detail="No phone number provided and ADMIN_WHATSAPP not set")

    now = datetime.now(COT).strftime("%d/%m/%Y %I:%M %p")
    test_msg = message or (
        f"\u2705 *SonIA Tracker \u2014 Test*\n\n"
        f"Este es un mensaje de prueba.\n"
        f"Si recibes esto, la conexion WhatsApp funciona correctamente.\n\n"
        f"Modulos activos: {', '.join(modules.keys())}\n\n"
        f"\U0001f916 SonIA Tracker \u2014 {now}"
    )

    try:
        sent = whatsapp.send_message_sync(target_phone, test_msg)
        return {
            "status": "sent" if sent else "failed",
            "phone": target_phone,
            "timestamp": now,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/test-odoo")
async def admin_test_odoo(tenant_number: int = None):
    """
    Test Odoo connectivity and tenant lookup.
    If tenant_number is provided, searches for that tenant.
    Otherwise, lists all companies.
    """
    odoo = modules.get("odoo")
    if not odoo:
        raise HTTPException(status_code=503, detail="Odoo module not available")

    try:
        auth_ok = odoo.authenticate()
        if not auth_ok:
            return {"status": "error", "detail": "Odoo authentication failed"}

        if tenant_number is not None:
            company = odoo.find_company_by_tenant_number(
                tenant_number, field_name=config.ODOO_TENANT_FIELD
            )
            if company:
                contacts = odoo.get_whatsapp_contacts_for_company(company["id"])
                return {
                    "status": "found",
                    "tenant_number": tenant_number,
                    "company": company,
                    "whatsapp_contacts": contacts,
                }
            else:
                return {
                    "status": "not_found",
                    "tenant_number": tenant_number,
                    "field_searched": config.ODOO_TENANT_FIELD,
                    "hint": "Verify the tenant field exists in Odoo and has the correct value",
                }
        else:
            companies = odoo.search_companies()
            return {
                "status": "ok",
                "auth": "success",
                "companies_found": len(companies),
                "companies": companies[:10],
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/test-flow")
async def admin_test_flow(tenant_number: int = None):
    """
    Run the daily flow for a single tenant (dry-run style test).
    If no tenant_number provided, reads DynamoDB and processes the first tenant found.
    """
    if not modules:
        raise HTTPException(status_code=503, detail="Modules not initialized")

    dynamo = modules.get("dynamo")
    if not dynamo:
        raise HTTPException(status_code=503, detail="DynamoDB not available")

    try:
        # Read from DynamoDB
        raw_shipments = dynamo.scan_all_shipments()
        if not raw_shipments:
            return {"status": "no_data", "detail": "No shipments in DynamoDB"}

        # Group by tenant
        tenant_groups = defaultdict(list)
        for reserve in raw_shipments:
            tid = reserve.get("tenant")
            if tid is not None:
                tenant_groups[int(tid)].append(reserve)

        # Pick the target tenant
        if tenant_number is not None:
            if tenant_number not in tenant_groups:
                return {
                    "status": "tenant_not_in_dynamo",
                    "tenant_number": tenant_number,
                    "available_tenants": list(tenant_groups.keys()),
                }
            target_tid = tenant_number
        else:
            target_tid = list(tenant_groups.keys())[0]

        reserves = tenant_groups[target_tid]

        # Count packages
        total_pkgs = sum(len(r.get("packages", [])) for r in reserves)
        active_pkgs = sum(
            1 for r in reserves for p in r.get("packages", [])
            if p.get("status", "").lower() != "delivered"
        )

        # Process just this tenant
        test_stats = {
            "total_shipments_read": len(raw_shipments),
            "tenants_found": len(tenant_groups),
            "tenants_in_odoo": 0,
            "tenants_missing_odoo": 0,
            "tenants_no_whatsapp": 0,
            "new_shipments": 0,
            "shipments_checked": 0,
            "shipments_updated": 0,
            "shipments_delivered": 0,
            "claims_created": 0,
            "reports_generated": 0,
            "reports_sent": 0,
            "alerts_sent": 0,
        }
        test_errors = []

        await _process_tenant(
            tenant_id=target_tid,
            reserves=reserves,
            modules=modules,
            stats=test_stats,
            errors=test_errors,
            total_active_packages=active_pkgs,
        )

        return {
            "status": "completed",
            "tenant_processed": target_tid,
            "reserves_count": len(reserves),
            "total_packages": total_pkgs,
            "active_packages": active_pkgs,
            "stats": test_stats,
            "errors": test_errors,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/seed-data")
async def admin_seed_data():
    """Seed initial BloomsPal client and tenant_mapping data."""
    db = modules.get("db")
    if not db:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        db.connect()

        # 1. Insert BloomsPal client
        db.cursor.execute("""
            INSERT INTO clients (name, dynamo_name, dynamo_tenant_id, is_active)
            VALUES ('BloomsPal', 'BloomsPal', 1, TRUE)
            ON CONFLICT DO NOTHING
            RETURNING id
        """)
        result = db.cursor.fetchone()
        client_id = result["id"] if result else None

        if not client_id:
            db.cursor.execute("SELECT id FROM clients WHERE name = 'BloomsPal'")
            result = db.cursor.fetchone()
            client_id = result["id"] if result else None

        # 2. Insert tenant_mapping
        db.cursor.execute("""
            INSERT INTO tenant_mapping (dynamo_tenant_id, client_id)
            VALUES (1, %s)
            ON CONFLICT (dynamo_tenant_id) DO NOTHING
            RETURNING *
        """, (client_id,))
        mapping = db.cursor.fetchone()

        # 3. Insert contacts
        contacts = [
            ("Johan", "573142285386"),
            ("Danny", "573105870328"),
            ("Carlos", "573108507879"),
        ]
        for name, phone in contacts:
            db.cursor.execute("""
                INSERT INTO client_contacts (client_id, name, whatsapp_number, is_active)
                VALUES (%s, %s, %s, TRUE)
                ON CONFLICT DO NOTHING
            """, (client_id, name, phone))

        db.conn.commit()
        db.close()

        return {
            "status": "success",
            "client_id": client_id,
            "tenant_mapping_id": mapping["id"] if mapping else "already existed",
            "contacts_added": len(contacts),
            "whatsapp_numbers": ['573142285386', '573105870328', '573108507879']
        }
    except Exception as e:
        if db.conn:
            db.conn.rollback()
        db.close()
        raise HTTPException(status_code=500, detail=str(e))
