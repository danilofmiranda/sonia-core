âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
â                    SonIA Core â Daily Tracking Orchestrator                    â
â                              BloomsPal                                        â
âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

Automated daily flow:
1. Read tracking numbers from DynamoDB (READ ONLY)
2. Check FedEx API for undelivered shipments
3. Store/update results in PostgreSQL
4. Detect anomalies and create proactive claims
5. Query Odoo for client contacts
6. Send reports via WhatsApp through SonIA Agent
7. Alert admin on inconsistencies

Schedule: Daily at 4:00 AM COT (UTC-5)
"""

import logging
import sys
import json
import os
import psycopg2
from datetime import datetime, timezone, timedelta, date
from contextlib import asynccontextmanager
from typing import Dict, List

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import config
from modules.dynamo_reader import DynamoReader
from modules.fedex_tracker import FedExTracker, get_sonia_status
from modules.db_manager import DBManager
from modules.odoo_client import OdooClient
from modules.whatsapp_sender import WhatsAppSender
from modules.report_generator import ReportGenerator
from modules.anomaly_detector import AnomalyDetector

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("sonia-core")

COT = timezone(timedelta(hours=-5))

# ============================================================================
# SCHEDULER
# ============================================================================

scheduler = BackgroundScheduler(timezone="America/Bogota")


def run_migration():
    """Run SQL migration on startup if tables don't exist."""
        database_url = os.getenv("DATABASE_URL")
            if not database_url:
                    logger.warning("DATABASE_URL not set, skipping migration")
                            return
                                
                                    try:
                                            conn = psycopg2.connect(database_url)
                                                    cur = conn.cursor()
                                                            
                                                                    # Check if migration already ran (check if shipments table exists)
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
# DAILY FLOW ORCHESTRATOR
# ============================================================================

def run_daily_flow(manual: bool = False):
    """
    Main daily orchestration flow.
    This is the core function that runs every day at 4 AM COT.
    """
    start_time = datetime.now(COT)
    trigger = "manual" if manual else "scheduled"
    logger.info(f"{'='*60}")
    logger.info(f"DAILY FLOW STARTED ({trigger}) at {start_time.strftime('%Y-%m-%d %H:%M:%S')} COT")
    logger.info(f"{'='*60}")

    # Initialize modules
    db = None
    fedex = None
    run_id = None
    unmapped_tenants = set()

    try:
        # ââ Initialize Database ââ
        db = DBManager(config.DATABASE_URL)
        db.connect()
        run_id = db.start_run(run_date=date.today())
        logger.info(f"Run ID: {run_id}")

        metrics = {
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

        # ââ STEP 1: Read from DynamoDB ââ
        logger.info("STEP 1: Reading from DynamoDB...")
        try:
            dynamo = DynamoReader(
                aws_access_key=config.AWS_ACCESS_KEY_ID,
                aws_secret_key=config.AWS_SECRET_ACCESS_KEY,
                region=config.AWS_REGION,
                table_name=config.DYNAMO_TABLE_RESERVES,
            )
            reserves = dynamo.scan_all_reserves()
            tracking_list = dynamo.extract_all_tracking_numbers(reserves)
            metrics["total_shipments_read"] = len(tracking_list)
            logger.info(f"Read {len(tracking_list)} tracking numbers from {len(reserves)} reserves")
        except Exception as e:
            logger.error(f"STEP 1 FAILED: {e}")
            errors.append({"step": "dynamo_read", "error": str(e)})
            if db and run_id:
                db.complete_run(run_id, "failed", metrics, errors)
            _send_failure_alert(f"Error leyendo DynamoDB: {e}")
            return

        # ââ STEP 2: Sync tracking numbers to PostgreSQL ââ
        logger.info("STEP 2: Syncing to PostgreSQL...")
        tenant_mapping = db.get_tenant_mapping()
        new_count = 0

        for item in tracking_list:
            tenant_id = item.get("tenant")

            # Get client info from tenant mapping
            if tenant_id not in tenant_mapping:
                unmapped_tenants.add(tenant_id)
                logger.warning(f"Tenant {tenant_id} not found in mapping, using fallback")
                client_info = {
                    "id": None,
                    "name": f"Unmapped-Tenant-{tenant_id}",
                    "odoo_company_id": None,
                }
            else:
                client_info = tenant_mapping[tenant_id]

            shipment_data = {
                "tracking_number": item["tracking_number"],
                "client_id": client_info.get("id"),
                "client_name_raw": client_info.get("name", f"Tenant-{tenant_id}"),
                "dynamo_data": json.dumps({
                    "reserve_id": item.get("reserve_id"),
                    "order_id": item.get("order_id"),
                    "package_id": item.get("package_id"),
                    "dynamo_status": item.get("dynamo_status"),
                    "tenant": tenant_id,
                }),
            }

            was_new = db.upsert_shipment(shipment_data)
            if was_new:
                new_count += 1

        metrics["new_shipments"] = new_count
        logger.info(f"Synced {len(tracking_list)} shipments ({new_count} new)")

        # Alert admin about unmapped tenants
        if unmapped_tenants:
            unmapped_list = ", ".join(str(t) for t in sorted(unmapped_tenants))
            alert_msg = f"â ï¸ *Tenants sin mapeo detectados*\n\nIDs: {unmapped_list}\n\nPor favor actualizar la tabla tenant_mapping."
            if config.ADMIN_WHATSAPP and config.SONIA_AGENT_URL:
                try:
                    whatsapp = WhatsAppSender(
                        agent_url=config.SONIA_AGENT_URL,
                        api_key=config.SONIA_AGENT_API_KEY,
                    )
                    whatsapp.send_alert_sync(config.ADMIN_WHATSAPP, alert_msg)
                    metrics["alerts_sent"] += 1
                except Exception as e:
                    logger.error(f"Failed to send unmapped tenants alert: {e}")

        # ââ STEP 3: Query FedEx for undelivered shipments ââ
        logger.info("STEP 3: Querying FedEx API...")
        undelivered = db.get_undelivered_shipments()
        logger.info(f"Found {len(undelivered)} undelivered shipments to check")

        if undelivered:
            try:
                fedex = FedExTracker(
                    client_id=config.FEDEX_API_KEY,
                    client_secret=config.FEDEX_SECRET_KEY,
                    account_number=config.FEDEX_ACCOUNT,
                    sandbox=False,
                )

                if not fedex.authenticate():
                    raise RuntimeError("FedEx authentication failed")

                # Process in batches
                batch_size = config.FEDEX_BATCH_SIZE
                updated_count = 0
                delivered_count = 0

                for i in range(0, len(undelivered), batch_size):
                    batch = undelivered[i:i + batch_size]
                    tracking_numbers = [s["tracking_number"] for s in batch]

                    results = fedex.track_batch(tracking_numbers)

                    for tn, result in results.items():
                        if result and not result.get("error"):
                            # Get the shipment to access all fields
                            shipment = next((s for s in batch if s["tracking_number"] == tn), None)
                            if shipment:
                                # Normalize status
                                sonia_status = get_sonia_status(
                                    result.get("status", "unknown"),
                                    result.get("status_detail", "")
                                )

                                # Extract delivery date and estimated delivery date
                                delivery_date = None
                                estimated_delivery_date = None

                                if result.get("latest_event"):
                                    event_date = result["latest_event"].get("date")
                                    if event_date and sonia_status == "delivered":
                                        delivery_date = event_date

                                if result.get("estimated_delivery"):
                                    estimated_delivery_date = result["estimated_delivery"]

                                # Extract destination info from latest event
                                destination_city = None
                                destination_state = None
                                destination_country = None

                                if result.get("latest_event"):
                                    loc = result["latest_event"].get("location", {})
                                    destination_city = loc.get("city")
                                    destination_state = loc.get("state")
                                    destination_country = loc.get("country")

                                update_data = {
                                    "tracking_number": tn,
                                    "sonia_status": sonia_status,
                                    "fedex_status": result.get("status_detail", ""),
                                    "fedex_status_code": result.get("status"),
                                    "delivery_date": delivery_date,
                                    "estimated_delivery_date": estimated_delivery_date,
                                    "destination_city": destination_city,
                                    "destination_state": destination_state,
                                    "destination_country": destination_country,
                                    "is_delivered": sonia_status == "delivered",
                                    "last_fedex_check": datetime.now(COT),
                                    "raw_fedex_response": json.dumps(result.get("raw_response", {})),
                                }

                                db.update_shipment_fedex_data(tn, update_data)
                                updated_count += 1

                                if sonia_status == "delivered":
                                    delivered_count += 1

                    # Respect rate limits
                    import time
                    if i + batch_size < len(undelivered):
                        time.sleep(config.FEDEX_BATCH_DELAY)

                metrics["shipments_checked"] = len(undelivered)
                metrics["shipments_updated"] = updated_count
                metrics["shipments_delivered"] = delivered_count
                logger.info(f"FedEx check complete: {updated_count} updated, {delivered_count} newly delivered")

            except Exception as e:
                logger.error(f"STEP 3 ERROR: {e}")
                errors.append({"step": "fedex_check", "error": str(e)})

        # ââ STEP 4: Detect anomalies (Part C) ââ
        logger.info("STEP 4: Detecting anomalies...")
        try:
            detector = AnomalyDetector(thresholds={
                "transit_days": config.THRESHOLD_TRANSIT_DAYS,
                "customs_days": config.THRESHOLD_CUSTOMS_DAYS,
                "delivery_attempt_days": config.THRESHOLD_DELIVERY_ATTEMPT_DAYS,
                "label_no_movement_days": config.THRESHOLD_LABEL_NO_MOVEMENT_DAYS,
            })

            # Get all undelivered shipments with updated status
            all_undelivered = db.get_undelivered_shipments()
            anomalies = detector.check_all_shipments(all_undelivered)

            claims_created = 0
            for anomaly in anomalies:
                tn = anomaly["tracking_number"]
                rule = anomaly["rule"]

                # Check if claim already exists for this tracking+rule
                if not db.claim_exists_for_tracking(tn, rule):
                    claim_data = {
                        "tracking_number": tn,
                        "client_id": anomaly.get("client_id"),
                        "client_name": anomaly.get("client_name", ""),
                        "claim_type": anomaly.get("claim_type", "otro"),
                        "description": anomaly.get("description", ""),
                        "origin": "proactivo_tracker",
                        "created_automatically": True,
                        "auto_detection_rule": rule,
                    }
                    db.create_claim(claim_data)
                    claims_created += 1
                    logger.info(f"Auto-claim created: {tn} ({rule})")

            metrics["claims_created"] = claims_created
            logger.info(f"Anomaly detection complete: {claims_created} new claims created")

        except Exception as e:
            logger.error(f"STEP 4 ERROR: {e}")
            errors.append({"step": "anomaly_detection", "error": str(e)})

        # ââ STEP 5: Query Odoo and send reports ââ
        logger.info("STEP 5: Querying Odoo and sending reports...")
        try:
            odoo = OdooClient(
                url=config.ODOO_URL,
                db=config.ODOO_DB,
                username=config.ODOO_USERNAME,
                password=config.ODOO_PASSWORD,
            )

            whatsapp = WhatsAppSender(
                agent_url=config.SONIA_AGENT_URL,
                api_key=config.SONIA_AGENT_API_KEY,
            )

            report_gen = ReportGenerator()

            if odoo.authenticate():
                # Process each client/tenant
                for tenant_id, client_info in tenant_mapping.items():
                    client_id = client_info.get("id")
                    client_name = client_info.get("name", f"Tenant-{tenant_id}")
                    odoo_company_id = client_info.get("odoo_company_id")

                    # Get shipments for this client
                    if client_id:
                        shipments = db.get_all_shipments_for_report(client_id)

                        if not shipments:
                            continue

                        # Generate report
                        report_text = report_gen.generate_client_report(client_name, shipments)
                        metrics["reports_generated"] += 1

                        # Get contacts from Odoo if we have odoo_company_id
                        if odoo_company_id:
                            try:
                                contacts = odoo.get_contacts_for_company(odoo_company_id)

                                for contact in contacts:
                                    phone = contact.get("whatsapp")
                                    if phone:
                                        success = whatsapp.send_report_sync(phone, report_text, client_name)
                                        if success:
                                            metrics["reports_sent"] += 1
                            except Exception as e:
                                logger.error(f"Error getting Odoo contacts for company {odoo_company_id}: {e}")
                                errors.append({"step": "odoo_contacts", "error": str(e)})
                        else:
                            # Client not in Odoo - alert admin
                            active_count = len([s for s in shipments if not s.get("is_delivered")])
                            alert = report_gen.generate_admin_inconsistency_alert(
                                client_name, tenant_id, active_count
                            )
                            if config.ADMIN_WHATSAPP:
                                whatsapp.send_alert_sync(config.ADMIN_WHATSAPP, alert)
                                metrics["alerts_sent"] += 1

                logger.info(f"Reports: {metrics['reports_generated']} generated, {metrics['reports_sent']} sent")
            else:
                logger.error("Failed to authenticate with Odoo")
                errors.append({"step": "odoo_auth", "error": "Authentication failed"})

        except Exception as e:
            logger.error(f"STEP 5 ERROR: {e}")
            errors.append({"step": "reports_and_whatsapp", "error": str(e)})

        # ââ STEP 6: Complete run ââ
        status = "success" if not errors else "partial"
        db.complete_run(run_id, status, metrics, errors)

        elapsed = (datetime.now(COT) - start_time).total_seconds()
        logger.info(f"{'='*60}")
        logger.info(f"DAILY FLOW COMPLETED ({status}) in {elapsed:.1f}s")
        logger.info(f"Metrics: {json.dumps({k:v for k,v in metrics.items()}, indent=2)}")
        if errors:
            logger.warning(f"Errors: {json.dumps(errors, indent=2)}")
        logger.info(f"{'='*60}")

    except Exception as e:
        logger.critical(f"CRITICAL ERROR in daily flow: {e}")
        import traceback
        traceback.print_exc()
        _send_failure_alert(f"Error crÃ­tico en flujo diario: {e}")
        if db and run_id:
            try:
                db.complete_run(run_id, "failed", {}, [{"step": "critical", "error": str(e)}])
            except:
                pass
    finally:
        if fedex:
            fedex.close()
        if db:
            db.close()


def _send_failure_alert(message: str):
    """Send failure alert to admin via WhatsApp."""
    try:
        if config.ADMIN_WHATSAPP and config.SONIA_AGENT_URL:
            whatsapp = WhatsAppSender(
                agent_url=config.SONIA_AGENT_URL,
                api_key=config.SONIA_AGENT_API_KEY,
            )
            whatsapp.send_alert_sync(config.ADMIN_WHATSAPP, message)
    except Exception as e:
        logger.error(f"Failed to send failure alert: {e}")


# ============================================================================
# FASTAPI APP
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    # Startup: start scheduler
        run_migration()
    scheduler.add_job(
        run_daily_flow,
        CronTrigger(hour=config.CRON_HOUR, minute=config.CRON_MINUTE),
        id="daily_flow",
        name="SonIA Daily Tracking Flow",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"Scheduler started. Next run at {config.CRON_HOUR}:{config.CRON_MINUTE:02d} COT")

    yield

    # Shutdown
    scheduler.shutdown()
    logger.info("Scheduler shut down")


app = FastAPI(
    title="SonIA Core â BloomsPal",
    description="Daily tracking orchestrator",
    lifespan=lifespan,
)


@app.get("/")
async def health():
    """Health check endpoint."""
    now = datetime.now(COT)
    jobs = scheduler.get_jobs()
    next_run = jobs[0].next_run_time if jobs else None

    return {
        "status": "ok",
        "service": "sonia-core",
        "time": now.isoformat(),
        "scheduler": {
            "running": scheduler.running,
            "next_run": next_run.isoformat() if next_run else None,
            "jobs": len(jobs),
        },
    }


@app.post("/api/trigger")
async def trigger_manual_run(api_key: str = ""):
    """Manually trigger the daily flow (for testing)."""
    if api_key != config.SONIA_AGENT_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    import threading
    thread = threading.Thread(target=run_daily_flow, kwargs={"manual": True})
    thread.start()

    return {"status": "started", "message": "Daily flow triggered manually"}


@app.get("/api/status")
async def get_status():
    """Get the status of the last run."""
    try:
        db = DBManager(config.DATABASE_URL)
        db.connect()
        # Get last run log from daily_run_logs table
        cursor = db.conn.cursor()
        cursor.execute(
            "SELECT id, run_date, started_at, completed_at, "
            "total_shipments_read, new_shipments, shipments_checked, "
            "shipments_updated, shipments_delivered, claims_created, "
            "reports_generated, reports_sent, alerts_sent, status, errors "
            "FROM daily_run_logs ORDER BY started_at DESC LIMIT 1"
        )
        row = cursor.fetchone()
        db.close()

        if row:
            columns = [desc[0] for desc in cursor.description]
            result = dict(zip(columns, row))
            # Parse JSON errors if present
            if result.get("errors"):
                try:
                    result["errors"] = json.loads(result["errors"])
                except:
                    pass
            return result
        return {"message": "No runs yet"}
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=config.PORT)
