"""
SonIA Core â PostgreSQL Database Manager
Handles all database operations for shipments, clients, claims, and run logs.
Uses psycopg2 for synchronous operations (batch job context).
Schema aligned with migrations/001_initial_schema.sql.
"""

import json
import logging
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from typing import List, Dict, Any, Optional
from datetime import datetime, date

logger = logging.getLogger(__name__)


class DBManager:
    """PostgreSQL database manager for SonIA core."""

    def __init__(self, database_url: str):
        """
        Initialize database manager with connection URL.

        Args:
            database_url: PostgreSQL connection URL
                         (e.g., postgresql://user:password@host:port/database)
        """
        self.database_url = database_url
        self.conn: Optional[psycopg2.extensions.connection] = None
        self.cursor: Optional[RealDictCursor] = None

        logger.info("DBManager initialized")

    def connect(self) -> bool:
        """
        Establish database connection.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.conn = psycopg2.connect(self.database_url)
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            logger.info("Database connection established")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False