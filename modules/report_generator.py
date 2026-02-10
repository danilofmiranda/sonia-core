"""
SonIA Core â Report Generator Module
Generates per-client tracking reports for WhatsApp delivery.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

COT = timezone(timedelta(hours=-5))


class ReportGenerator:
    """Generates tracking reports for each client."""
