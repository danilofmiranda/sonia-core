"""
SonIA Core â Configuration
All settings loaded from environment variables for easy migration to AWS.
"""

import os
from datetime import timezone, timedelta

# ============================================================================
# TIMEZONE
# ============================================================================
COT = timezone(timedelta(hours=-5))  # Colombia Time

# ============================================================================
# DATABASE â PostgreSQL (Railway)
# ============================================================================
DATABASE_URL = os.getenv("DATABASE_URL", "")
# Railway provides DATABASE_URL. For local dev, set manually.

# ============================================================================
# AWS â DynamoDB (READ ONLY)
# ============================================================================
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
DYNAMO_TABLE_RESERVES = os.getenv("DYNAMO_TABLE_RESERVES", "reserves")

# ============================================================================
# FedEx API
# ============================================================================
FEDEX_API_KEY = os.getenv("FEDEX_API_KEY", "")
FEDEX_SECRET_KEY = os.getenv("FEDEX_SECRET_KEY", "")
FEDEX_ACCOUNT = os.getenv("FEDEX_ACCOUNT", "")
FEDEX_BASE_URL = os.getenv("FEDEX_BASE_URL", "https://apis.fedex.com")

# ============================================================================
# Odoo
# ============================================================================
ODOO_URL = os.getenv("ODOO_URL", "")
ODOO_DB = os.getenv("ODOO_DB", "")
ODOO_USERNAME = os.getenv("ODOO_USERNAME", "")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD", "")

# ============================================================================
# SonIA Agent (WhatsApp sender)
# ============================================================================
SONIA_AGENT_URL = os.getenv("SONIA_AGENT_URL", "")  # e.g., https://sonia-agent-production.up.railway.app
SONIA_AGENT_API_KEY = os.getenv("SONIA_AGENT_API_KEY", "")

# ============================================================================
# Admin notification
# ============================================================================
ADMIN_WHATSAPP = os.getenv("ADMIN_WHATSAPP", "")  # Danny's WhatsApp number

# ============================================================================
# Anomaly Detection Thresholds (configurable)
# ============================================================================
THRESHOLD_TRANSIT_DAYS = int(os.getenv("THRESHOLD_TRANSIT_DAYS", "7"))
THRESHOLD_CUSTOMS_DAYS = int(os.getenv("THRESHOLD_CUSTOMS_DAYS", "5"))
THRESHOLD_DELIVERY_ATTEMPT_DAYS = int(os.getenv("THRESHOLD_DELIVERY_ATTEMPT_DAYS", "2"))
THRESHOLD_LABEL_NO_MOVEMENT_DAYS = int(os.getenv("THRESHOLD_LABEL_NO_MOVEMENT_DAYS", "5"))

# ============================================================================
# Cron Schedule
# ============================================================================
CRON_HOUR = int(os.getenv("CRON_HOUR", "4"))  # 4 AM COT
CRON_MINUTE = int(os.getenv("CRON_MINUTE", "0"))

# ============================================================================
# FedEx API Batch Settings
# ============================================================================
FEDEX_BATCH_SIZE = int(os.getenv("FEDEX_BATCH_SIZE", "30"))  # Max 30 per FedEx recommendation
FEDEX_BATCH_DELAY = float(os.getenv("FEDEX_BATCH_DELAY", "0.5"))  # Seconds between batches

# ============================================================================
# Server
# ============================================================================
PORT = int(os.getenv("PORT", "8080"))
