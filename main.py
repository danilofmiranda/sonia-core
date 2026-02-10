"""
SonIA Core - Daily Tracking Orchestrator
BloomsPal / Fase 1

Runs daily at 4:00 AM COT (UTC-5):
1. Read shipments from DynamoDB
2. Match clients via tenant_mapping
3. Query FedEx Track API for status updates
4. Store/update in PostgreSQL
5. Detect anomalies and create proactive claims
6. Generate per-client reports
7. Send reports via WhatsApp (through SonIA Agent)
"""

import os
import logging
import psycopg2
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from typing import Dict, List, Any

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

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("sonia-core")

# Timezone
COT = timezone(timedelta(hours=-5))


# ============================================================================
# AUTO-MIGRATION
# ============================================================================

def run_migration():
    """Run SQL migration on startup if tables don't exist."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.warning("DATABASE_URL not set, skipping migration")
        return

    try:
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()

        # Check if migration already ran
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'shipments'
            );
        """)
        exists = cur.fetchone()[0]

        if exists:
            logger.info("Database tables already exist, skipping migration")
            cur.close()
            conn.close()
            return

        # Read and execute migration file
        migration_path = os.path.join(os.path.dirname(__file__), "migrations", "001_initial_schema.sql")
        if os.path.exists(migration_path):
            with open(migration_path, "r") as f:
                sql = f.read()
            cur.execute(sql)
            conn.commit()
            logger.info("Migration 001_initial_schema.sql executed successfully!")
        else:
            logger.warning(f"Migration file not found: {migration_path}")

        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Migration failed: {e}")


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    DATABASE_URL = os.getenv("DATABASE_URL", "")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
    DYNAMO_TABLE = os.getenv("DYNAMO_TABLE_RESERVES", "reserves")
    FEDEX_API_KEY = os.getenv("FEDEX_API_KEY", "")
    FEDEX_SECRET_KEY = os.getenv("FEDEX_SECRET_KEY", "")
    FEDEX_ACCOUNT = os.getenv("FEDEX_ACCOUNT", "")
    ODOO_URL = os.getenv("ODOO_URL", "")
    ODOO_DB = os.getenv("ODOO_DB", "")
    ODOO_USER = os.getenv("ODOO_USER", "")
    ODOO_PASSWORD = os.getenv("ODOO_PASSWORD", "")
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
    modules = {}

    # Database
    if config.DATABASE_URL:
        modules["db"] = DBManager(config.DATABASE_URL)
        logger.info("DBManager initialized")
    else:
        logger.warning("DATABASE_URL not set - DB features disabled")

    # DynamoDB Reader
    if config.AWS_ACCESS_KEY_ID:
        modules["dynamo"] = DynamoReader(
            aws_access_key=config.AWS_ACCESS_KEY_ID,
            aws_secret_key=config.AWS_SECRET_ACCESS_KEY,
            region=config.AWS_REGION,
            table_name=config.DYNAMO_TABLE,
        )
        logger.info("DynamoReader initialized")

    # FedEx Tracker
    if config.FEDEX_API_KEY:
        modules["fedex"] = FedExTracker(
            client_id=config.FEDEX_API_KEY,
            client_secret=config.FEDEX_SECRET_KEY,
            account_number=config.FEDEX_ACCOUNT,
        )
        logger.info("FedExTracker initialized")

    # Anomaly Detector
    modules["anomaly"] = AnomalyDetector()
    logger.info("AnomalyDetector initialized")

    # Report Generator
    modules["reports"] = ReportGenerator()
    logger.info("ReportGenerator initialized")

    # WhatsApp Sender
    if config.SONIA_AGENT_URL:
        modules["whatsapp"] = WhatsAppSender(
            agent_url=config.SONIA_AGENT_URL,
            api_key=config.SONIA_AGENT_API_KEY,
        )
        logger.info("WhatsAppSender initialized")

    # Odoo Client
    if config.ODOO_URL and config.ODOO_USER:
        modules["odoo"] = OdooClient(
            url=config.ODOO_URL,
            db=config.ODOO_DB,
            username=config.ODOO_USER,
            password=config.ODOO_PASSWORD,
        )
        logger.info("OdooClient initialized")

    return modules


# ============================================================================
# DAILY ORCHESTRATION FLOW
# ============================================================================

async def daily_flow(modules: dict):
    """
    Main daily orchestration:
    1. Read from DynamoDB
    2. Match clients
    3. Query FedEx
    4. Update PostgreSQL
    5. Detect anomalies
    6. Generate reports
    7. Send via WhatsApp
    """
    db = modules.get("db")
    dynamo = modules.get("dynamo")
    fedex = modules.get("fedex")
    anomaly_detector = modules.get("anomaly")
    report_gen = modules.get("reports")
    whatsapp = modules.get("whatsapp")

    if not db:
        logger.error("DB not available, cannot run daily flow")
        return

    now = datetime.now(COT)
    logger.info(f"=== Starting daily flow at {now.strftime('%Y-%m-%d %H:%M:%S')} COT ===")

    # Create run log
    run_id = db.create_run_log(now.date())
    stats = {
        "total_shipments_read": 0,
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

    try:
        # ── Step 1: Read from DynamoDB ──
        logger.info("Step 1: Reading shipments from DynamoDB...")
        if dynamo:
            try:
                raw_shipments = dynamo.scan_all_shipments()
                stats["total_shipments_read"] = len(raw_shipments)
                logger.info(f"Read {len(raw_shipments)} shipments from DynamoDB")

                # Store new shipments in PostgreSQL
                for raw in raw_shipments:
                    tracking = raw.get("tracking_number") or raw.get("trackingNumber", "")
                    if not tracking:
                        continue
                    tenant_id = raw.get("tenant_id") or raw.get("tenantId")
                    client_info = db.get_client_by_tenant(tenant_id) if tenant_id else None

                    inserted = db.upsert_shipment(
                        tracking_number=tracking,
                        client_id=client_info["id"] if client_info else None,
                        client_name_raw=raw.get("client_name", ""),
                        dynamo_data=raw,
                    )
                    if inserted:
                        stats["new_shipments"] += 1
            except Exception as e:
                logger.error(f"DynamoDB read error: {e}")
                errors.append({"step": "dynamo_read", "error": str(e)})

        # ── Step 2: Get active shipments to check ──
        logger.info("Step 2: Getting active shipments...")
        active_shipments = db.get_active_shipments()
        logger.info(f"Found {len(active_shipments)} active shipments to check")

        # ── Step 3: Query FedEx ──
        logger.info("Step 3: Querying FedEx Track API...")
        if fedex and active_shipments:
            tracking_numbers = [s["tracking_number"] for s in active_shipments]

            # Process in batches of 30 (FedEx limit)
            batch_size = 30
            for i in range(0, len(tracking_numbers), batch_size):
                batch = tracking_numbers[i:i + batch_size]
                try:
                    results = fedex.track_batch(batch)
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
                    logger.error(f"FedEx batch error: {e}")
                    errors.append({"step": "fedex_track", "error": str(e), "batch": i})

        # ── Step 4: Detect anomalies ──
        logger.info("Step 4: Detecting anomalies...")
        if anomaly_detector:
            try:
                refreshed_shipments = db.get_active_shipments()
                shipment_dicts = [dict(s) for s in refreshed_shipments]
                anomalies = anomaly_detector.check_all_shipments(shipment_dicts)

                for anomaly in anomalies:
                    claim_id = db.create_proactive_claim(
                        tracking_number=anomaly["tracking_number"],
                        shipment_id=anomaly.get("shipment_id"),
                        client_id=anomaly.get("client_id"),
                        claim_type=anomaly["claim_type"],
                        description=anomaly["description"],
                        rule=anomaly["rule"],
                    )
                    if claim_id:
                        stats["claims_created"] += 1

                        # Send anomaly alert
                        if whatsapp and config.ADMIN_WHATSAPP:
                            alert_msg = report_gen.generate_anomaly_alert(
                                tracking_number=anomaly["tracking_number"],
                                client_name=anomaly.get("client_name", ""),
                                rule=anomaly["rule"],
                                details=anomaly.get("description", ""),
                            )
                            try:
                                await whatsapp.send_message(config.ADMIN_WHATSAPP, alert_msg)
                                stats["alerts_sent"] += 1
                            except Exception as e:
                                logger.error(f"WhatsApp alert error: {e}")
            except Exception as e:
                logger.error(f"Anomaly detection error: {e}")
                errors.append({"step": "anomaly_detect", "error": str(e)})

        # ── Step 5: Generate reports ──
        logger.info("Step 5: Generating client reports...")
        if report_gen:
            try:
                clients = db.get_all_clients_with_shipments()
                for client in clients:
                    client_shipments = db.get_shipments_by_client(client["id"])
                    if not client_shipments:
                        continue

                    report = report_gen.generate_client_report(
                        client_name=client["name"],
                        shipments=[dict(s) for s in client_shipments],
                    )
                    stats["reports_generated"] += 1

                    # ── Step 6: Send via WhatsApp ──
                    if whatsapp:
                        contacts = db.get_client_contacts(client["id"])
                        for contact in contacts:
                            if contact.get("whatsapp_number"):
                                try:
                                    await whatsapp.send_message(
                                        contact["whatsapp_number"], report
                                    )
                                    stats["reports_sent"] += 1
                                except Exception as e:
                                    logger.error(f"WhatsApp send error: {e}")
            except Exception as e:
                logger.error(f"Report generation error: {e}")
                errors.append({"step": "report_gen", "error": str(e)})

        # ── Finalize ──
        status = "success" if not errors else "partial"
        db.update_run_log(run_id, stats, errors, status)
        logger.info(f"=== Daily flow completed: {status} | Stats: {stats} ===")

    except Exception as e:
        logger.error(f"Daily flow critical error: {e}")
        if db and run_id:
            errors.append({"step": "critical", "error": str(e)})
            db.update_run_log(run_id, stats, errors, "failed")


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
    description="Daily tracking orchestrator for BloomsPal",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/")
async def root():
    return {
        "service": "SonIA Core",
        "status": "running",
        "environment": config.ENVIRONMENT,
        "version": "1.0.0",
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
        latest_run = db.get_latest_run_log()
        return latest_run if latest_run else {"message": "No runs yet"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ADMIN ENDPOINTS (temporal - para setup inicial)
# ============================================================================

@app.get("/admin/dynamo-scan")
async def admin_dynamo_scan():
    """Scan DynamoDB to see sample records and identify tenant IDs."""
    dynamo = modules.get("dynamo")
    if not dynamo:
        raise HTTPException(status_code=503, detail="DynamoDB not available")
    
    try:
        # Use the client directly to scan
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
            INSERT INTO tenant_mapping (dynamo_tenant_id, client_id, client_name, whatsapp_numbers, is_active, notes)
            VALUES (
                1,
                %s,
                'BloomsPal',
                ARRAY['573142285386', '573105870328', '573108507879'],
                TRUE,
                'Carlos Miranda (Carl), Danny Miranda, Andrea Ayala - desde WHATSAPP BBDD Odoo'
            )
            ON CONFLICT (dynamo_tenant_id) DO UPDATE SET
                whatsapp_numbers = EXCLUDED.whatsapp_numbers,
                notes = EXCLUDED.notes,
                updated_at = NOW()
            RETURNING id
        """, (client_id,))
        
        mapping = db.cursor.fetchone()
        
        # 3. Insert client_contacts
        contacts = [
            ('Carlos Miranda (Carl)', '573142285386'),
            ('Danilo Miranda de la Espriella (Danny)', '573105870328'),
            ('Andrea Ayala', '573108507879'),
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


@app.get("/admin/db-status")
async def admin_db_status():
    """Check database tables and record counts."""
    db = modules.get("db")
    if not db:
        raise HTTPException(status_code=503, detail="Database not available")
    
    try:
        db.connect()
        tables = ['clients', 'tenant_mapping', 'client_contacts', 'shipments', 'claims', 'daily_run_logs']
        counts = {}
        for table in tables:
            db.cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            result = db.cursor.fetchone()
            counts[table] = result["count"] if result else 0
        
        # Get tenant_mapping details
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
