"""
SonIA Core — PostgreSQL Database Manager
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
        except psycopg2.Error as e:
            logger.error(f"Database connection failed: {e}")
            return False

    def close(self):
        """Close database connection and cleanup."""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            logger.info("Database connection closed")
        except psycopg2.Error as e:
            logger.error(f"Error closing database connection: {e}")


    def health_check(self) -> bool:
        """Quick database health check."""
        try:
            conn = psycopg2.connect(self.database_url)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            conn.close()
            return True
        except Exception:
            return False


    def _ensure_connection(self) -> bool:
        """
        Ensure database connection is active; reconnect if needed.

        Returns:
            True if connection is valid, False otherwise
        """
        if self.conn is None or self.conn.closed:
            return self.connect()
        return True

    # ========== TENANT MAPPING OPERATIONS ==========

    def get_tenant_mapping(self) -> Dict[int, Dict[str, Any]]:
        """
        Get all tenant mapping information from tenant_mapping table.

        Returns:
            Dict mapping {dynamo_tenant_id: {client_id, client_name, odoo_company_id, ...}}
        """
        if not self._ensure_connection():
            return {}

        try:
            query = """
            SELECT
                dynamo_tenant_id,
                client_id,
                client_name,
                odoo_company_id,
                whatsapp_numbers,
                is_active,
                notes,
                created_at,
                updated_at
            FROM tenant_mapping
            ORDER BY dynamo_tenant_id
            """

            self.cursor.execute(query)
            results = self.cursor.fetchall()

            mapping = {}
            for row in results:
                row_dict = dict(row)
                dynamo_tenant_id = row_dict.pop("dynamo_tenant_id")
                mapping[dynamo_tenant_id] = row_dict

            logger.info(f"Retrieved tenant mapping for {len(mapping)} tenants")
            return mapping

        except psycopg2.Error as e:
            logger.error(f"Error getting tenant mapping: {e}")
            return {}

    def update_tenant_mapping(self, dynamo_tenant_id: int, data: Dict[str, Any]) -> bool:
        """
        Update a tenant mapping entry.

        Args:
            dynamo_tenant_id: DynamoDB tenant ID
            data: Dict with keys: client_id, client_name, odoo_company_id,
                  whatsapp_numbers, is_active, notes

        Returns:
            True if successful, False otherwise
        """
        if not self._ensure_connection():
            return False

        try:
            query = """
            UPDATE tenant_mapping
            SET
                client_id = COALESCE(%s, client_id),
                client_name = COALESCE(%s, client_name),
                odoo_company_id = COALESCE(%s, odoo_company_id),
                whatsapp_numbers = COALESCE(%s, whatsapp_numbers),
                is_active = COALESCE(%s, is_active),
                notes = COALESCE(%s, notes)
            WHERE dynamo_tenant_id = %s
            """

            self.cursor.execute(query, (
                data.get("client_id"),
                data.get("client_name"),
                data.get("odoo_company_id"),
                data.get("whatsapp_numbers"),
                data.get("is_active"),
                data.get("notes"),
                dynamo_tenant_id
            ))
            self.conn.commit()

            if self.cursor.rowcount > 0:
                logger.info(f"Tenant mapping updated: dynamo_tenant_id={dynamo_tenant_id}")
                return True
            else:
                logger.warning(f"Tenant mapping not found: {dynamo_tenant_id}")
                return False

        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error updating tenant mapping: {e}")
            return False

    # ========== CLIENT OPERATIONS ==========

    def upsert_client(self, data: Dict[str, Any]) -> bool:
        """
        Insert or update a client record.

        Expected data keys:
        - odoo_id: Odoo company/customer ID
        - name: Client/brand name
        - dynamo_name: Name as it appears in DynamoDB
        - dynamo_tenant_id: DynamoDB tenant ID
        - is_active: Boolean flag (default True)

        Args:
            data: Client data dict

        Returns:
            True if successful, False otherwise
        """
        if not self._ensure_connection():
            return False

        try:
            query = """
            INSERT INTO clients (
                odoo_id, name, dynamo_name, dynamo_tenant_id, is_active
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (odoo_id) DO UPDATE SET
                name = EXCLUDED.name,
                dynamo_name = EXCLUDED.dynamo_name,
                dynamo_tenant_id = EXCLUDED.dynamo_tenant_id,
                is_active = EXCLUDED.is_active
            """

            self.cursor.execute(query, (
                data.get("odoo_id"),
                data.get("name"),
                data.get("dynamo_name"),
                data.get("dynamo_tenant_id"),
                data.get("is_active", True)
            ))
            self.conn.commit()

            logger.info(f"Client upserted: odoo_id={data.get('odoo_id')}, name={data.get('name')}")
            return True

        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error upserting client: {e}")
            return False

    # ========== SHIPMENT OPERATIONS ==========

    def upsert_shipment(self, data: Dict[str, Any]) -> bool:
        """
        Insert or update a shipment by tracking_number using UPSERT logic.

        Expected data keys:
        - tracking_number (required)
        - client_id
        - client_name_raw
        - sonia_status: shipment_status enum value
        - fedex_status
        - fedex_status_code
        - label_creation_date
        - ship_date
        - delivery_date
        - estimated_delivery_date
        - destination_city
        - destination_state
        - destination_country
        - origin_city
        - origin_state
        - origin_country
        - is_delivered
        - last_fedex_check
        - last_status_change
        - fedex_check_count
        - raw_fedex_response: JSON object
        - dynamo_data: JSON object

        Args:
            data: Shipment data dict

        Returns:
            True if successful, False otherwise
        """
        if not self._ensure_connection():
            return False

        try:
            tracking_number = data.get("tracking_number")
            if not tracking_number:
                logger.error("tracking_number is required for upsert_shipment")
                return False

            query = """
            INSERT INTO shipments (
                tracking_number, client_id, client_name_raw, sonia_status,
                fedex_status, fedex_status_code, label_creation_date, ship_date,
                delivery_date, estimated_delivery_date, destination_city,
                destination_state, destination_country, origin_city, origin_state,
                origin_country, is_delivered, last_fedex_check, last_status_change,
                fedex_check_count, raw_fedex_response, dynamo_data
            )
            VALUES (
                %s, %s, %s, %s::shipment_status, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (tracking_number) DO UPDATE SET
                client_id = EXCLUDED.client_id,
                client_name_raw = EXCLUDED.client_name_raw,
                sonia_status = EXCLUDED.sonia_status,
                fedex_status = EXCLUDED.fedex_status,
                fedex_status_code = EXCLUDED.fedex_status_code,
                label_creation_date = EXCLUDED.label_creation_date,
                ship_date = EXCLUDED.ship_date,
                delivery_date = EXCLUDED.delivery_date,
                estimated_delivery_date = EXCLUDED.estimated_delivery_date,
                destination_city = EXCLUDED.destination_city,
                destination_state = EXCLUDED.destination_state,
                destination_country = EXCLUDED.destination_country,
                origin_city = EXCLUDED.origin_city,
                origin_state = EXCLUDED.origin_state,
                origin_country = EXCLUDED.origin_country,
                is_delivered = EXCLUDED.is_delivered,
                last_fedex_check = EXCLUDED.last_fedex_check,
                last_status_change = EXCLUDED.last_status_change,
                fedex_check_count = EXCLUDED.fedex_check_count,
                raw_fedex_response = EXCLUDED.raw_fedex_response,
                dynamo_data = EXCLUDED.dynamo_data
            """

            raw_fedex = data.get("raw_fedex_response")
            dynamo = data.get("dynamo_data")

            self.cursor.execute(query, (
                tracking_number,
                data.get("client_id"),
                data.get("client_name_raw"),
                data.get("sonia_status", "unknown"),
                data.get("fedex_status"),
                data.get("fedex_status_code"),
                data.get("label_creation_date"),
                data.get("ship_date"),
                data.get("delivery_date"),
                data.get("estimated_delivery_date"),
                data.get("destination_city"),
                data.get("destination_state"),
                data.get("destination_country"),
                data.get("origin_city"),
                data.get("origin_state"),
                data.get("origin_country"),
                data.get("is_delivered", False),
                data.get("last_fedex_check"),
                data.get("last_status_change"),
                data.get("fedex_check_count", 0),
                Json(raw_fedex) if raw_fedex else None,
                Json(dynamo) if dynamo else None
            ))

            self.conn.commit()
            logger.debug(f"Shipment upserted: {tracking_number}")
            return True

        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error upserting shipment: {e}")
            return False

    def get_undelivered_shipments(self) -> List[Dict[str, Any]]:
        """
        Get all shipments where is_delivered = False.

        Returns:
            List of shipment dicts
        """
        if not self._ensure_connection():
            return []

        try:
            query = """
            SELECT * FROM shipments
            WHERE is_delivered = FALSE
            ORDER BY updated_at DESC
            """

            self.cursor.execute(query)
            results = self.cursor.fetchall()
            logger.info(f"Retrieved {len(results)} undelivered shipments")
            return [dict(row) for row in results]

        except psycopg2.Error as e:
            logger.error(f"Error getting undelivered shipments: {e}")
            return []

    def get_shipments_by_client(self, client_id: int) -> List[Dict[str, Any]]:
        """
        Get all shipments for a specific client.

        Args:
            client_id: Client ID from clients table

        Returns:
            List of shipment dicts
        """
        if not self._ensure_connection():
            return []

        try:
            query = """
            SELECT * FROM shipments
            WHERE client_id = %s
            ORDER BY updated_at DESC
            """

            self.cursor.execute(query, (client_id,))
            results = self.cursor.fetchall()
            logger.info(f"Retrieved {len(results)} shipments for client {client_id}")
            return [dict(row) for row in results]

        except psycopg2.Error as e:
            logger.error(f"Error getting shipments by client: {e}")
            return []

    def get_all_shipments_for_report(self, client_id: int) -> List[Dict[str, Any]]:
        """
        Get all shipments (delivered and undelivered) for a client.
        Used for generating reports.

        Args:
            client_id: Client ID from clients table

        Returns:
            List of shipment dicts
        """
        if not self._ensure_connection():
            return []

        try:
            query = """
            SELECT * FROM shipments
            WHERE client_id = %s
            ORDER BY created_at DESC
            """

            self.cursor.execute(query, (client_id,))
            results = self.cursor.fetchall()
            logger.info(f"Retrieved {len(results)} shipments for report (client {client_id})")
            return [dict(row) for row in results]

        except psycopg2.Error as e:
            logger.error(f"Error getting shipments for report: {e}")
            return []

    def update_shipment_fedex_data(self, tracking_number: str, data: Dict[str, Any]) -> bool:
        """
        Update FedEx-related fields for a shipment after API call.

        Expected data keys:
        - sonia_status: shipment_status enum value
        - fedex_status
        - fedex_status_code
        - delivery_date
        - estimated_delivery_date
        - is_delivered
        - last_fedex_check
        - last_status_change
        - fedex_check_count
        - raw_fedex_response: JSON object

        Args:
            tracking_number: FedEx tracking number
            data: Fields to update

        Returns:
            True if successful, False otherwise
        """
        if not self._ensure_connection():
            return False

        try:
            query = """
            UPDATE shipments
            SET
                sonia_status = COALESCE(%s::shipment_status, sonia_status),
                fedex_status = COALESCE(%s, fedex_status),
                fedex_status_code = COALESCE(%s, fedex_status_code),
                delivery_date = COALESCE(%s, delivery_date),
                estimated_delivery_date = COALESCE(%s, estimated_delivery_date),
                is_delivered = COALESCE(%s, is_delivered),
                last_fedex_check = COALESCE(%s, last_fedex_check),
                last_status_change = COALESCE(%s, last_status_change),
                fedex_check_count = COALESCE(%s, fedex_check_count),
                raw_fedex_response = COALESCE(%s, raw_fedex_response)
            WHERE tracking_number = %s
            """

            raw_fedex = data.get("raw_fedex_response")

            self.cursor.execute(query, (
                data.get("sonia_status"),
                data.get("fedex_status"),
                data.get("fedex_status_code"),
                data.get("delivery_date"),
                data.get("estimated_delivery_date"),
                data.get("is_delivered"),
                data.get("last_fedex_check"),
                data.get("last_status_change"),
                data.get("fedex_check_count"),
                Json(raw_fedex) if raw_fedex else None,
                tracking_number
            ))

            self.conn.commit()

            if self.cursor.rowcount > 0:
                logger.debug(f"Shipment FedEx data updated: {tracking_number}")
                return True
            else:
                logger.warning(f"Shipment not found: {tracking_number}")
                return False

        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error updating shipment FedEx data: {e}")
            return False

    # ========== CLAIM OPERATIONS ==========

    def create_claim(self, data: Dict[str, Any]) -> Optional[int]:
        """
        Create a new claim record.

        Expected data keys:
        - tracking_number
        - shipment_id
        - client_id
        - client_name
        - claim_type: claim_type enum value (no_entregado, danado, perdida_total, etc.)
        - claim_type_other: string if claim_type = 'otro'
        - description
        - status: claim_status enum (default 'nuevo')
        - origin: claim_origin enum (default 'manual')
        - assigned_to_team
        - assigned_to_fedex_exec
        - created_automatically
        - auto_detection_rule

        Args:
            data: Claim data dict

        Returns:
            Claim ID if successful, None otherwise
        """
        if not self._ensure_connection():
            return None

        try:
            query = """
            INSERT INTO claims (
                tracking_number, shipment_id, client_id, client_name, claim_type,
                claim_type_other, description, status, origin, assigned_to_team,
                assigned_to_fedex_exec, created_automatically, auto_detection_rule
            )
            VALUES (
                %s, %s, %s, %s, %s::claim_type, %s, %s, %s::claim_status,
                %s::claim_origin, %s, %s, %s, %s
            )
            RETURNING id
            """

            self.cursor.execute(query, (
                data.get("tracking_number"),
                data.get("shipment_id"),
                data.get("client_id"),
                data.get("client_name"),
                data.get("claim_type"),
                data.get("claim_type_other"),
                data.get("description"),
                data.get("status", "nuevo"),
                data.get("origin", "manual"),
                data.get("assigned_to_team"),
                data.get("assigned_to_fedex_exec"),
                data.get("created_automatically", False),
                data.get("auto_detection_rule")
            ))

            claim_id = self.cursor.fetchone()[0]
            self.conn.commit()

            logger.info(f"Claim created: id={claim_id}, tracking_number={data.get('tracking_number')}")
            return claim_id

        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error creating claim: {e}")
            return None

    def claim_exists_for_tracking(self, tracking_number: str, rule: str) -> bool:
        """
        Check if an auto-claim already exists for a tracking number and rule.

        Args:
            tracking_number: FedEx tracking number
            rule: Auto-detection rule identifier

        Returns:
            True if auto-claim exists, False otherwise
        """
        if not self._ensure_connection():
            return False

        try:
            query = """
            SELECT COUNT(*) as count FROM claims
            WHERE tracking_number = %s
              AND auto_detection_rule = %s
              AND created_automatically = TRUE
            """

            self.cursor.execute(query, (tracking_number, rule))
            result = self.cursor.fetchone()
            exists = result["count"] > 0

            if exists:
                logger.debug(f"Auto-claim exists: {tracking_number} / {rule}")
            return exists

        except psycopg2.Error as e:
            logger.error(f"Error checking claim existence: {e}")
            return False

    def add_claim_history(
        self,
        claim_id: int,
        status_from: Optional[str],
        status_to: str,
        changed_by_name: str,
        notes: Optional[str] = None
    ) -> bool:
        """
        Add a history entry for a claim status change.

        Args:
            claim_id: Claim ID
            status_from: Previous status (claim_status enum or NULL)
            status_to: New status (claim_status enum)
            changed_by_name: Name of who made the change
            notes: Optional notes about the change

        Returns:
            True if successful, False otherwise
        """
        if not self._ensure_connection():
            return False

        try:
            query = """
            INSERT INTO claim_history (
                claim_id, status_from, status_to, changed_by_name, notes
            )
            VALUES (
                %s, %s::claim_status, %s::claim_status, %s, %s
            )
            """

            self.cursor.execute(query, (
                claim_id,
                status_from,
                status_to,
                changed_by_name,
                notes
            ))

            self.conn.commit()
            logger.debug(f"Claim history added: claim_id={claim_id}, {status_from} -> {status_to}")
            return True

        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error adding claim history: {e}")
            return False

    # ========== RUN LOG OPERATIONS ==========

    def start_run(self, run_date: date) -> Optional[int]:
        """
        Start a new batch run and log it to daily_run_logs.

        Args:
            run_date: Date of the run

        Returns:
            Run log ID if successful, None otherwise
        """
        if not self._ensure_connection():
            return None

        try:
            query = """
            INSERT INTO daily_run_logs (
                run_date, started_at, status
            )
            VALUES (%s, %s, %s::run_status)
            RETURNING id
            """

            self.cursor.execute(query, (
                run_date,
                datetime.utcnow(),
                "running"
            ))

            run_id = self.cursor.fetchone()[0]
            self.conn.commit()

            logger.info(f"Run started: run_id={run_id}, run_date={run_date}")
            return run_id

        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error starting run: {e}")
            return None

    def update_run_metrics(self, run_id: int, metrics_dict: Dict[str, Any]) -> bool:
        """
        Update specific metric columns for a run.

        Supported keys in metrics_dict:
        - total_shipments_read
        - new_shipments
        - shipments_checked
        - shipments_updated
        - shipments_delivered
        - claims_created
        - reports_generated
        - reports_sent
        - alerts_sent

        Args:
            run_id: Run log ID
            metrics_dict: Dict with metric names as keys

        Returns:
            True if successful, False otherwise
        """
        if not self._ensure_connection():
            return False

        try:
            # Build dynamic SET clause for provided metrics
            set_clauses = []
            params = []
            valid_metrics = [
                "total_shipments_read", "new_shipments", "shipments_checked",
                "shipments_updated", "shipments_delivered", "claims_created",
                "reports_generated", "reports_sent", "alerts_sent"
            ]

            for key, value in metrics_dict.items():
                if key in valid_metrics and value is not None:
                    set_clauses.append(f"{key} = %s")
                    params.append(value)

            if not set_clauses:
                logger.warning(f"No valid metrics provided for run {run_id}")
                return False

            query = f"""
            UPDATE daily_run_logs
            SET {', '.join(set_clauses)}
            WHERE id = %s
            """
            params.append(run_id)

            self.cursor.execute(query, params)
            self.conn.commit()

            if self.cursor.rowcount > 0:
                logger.debug(f"Run metrics updated: run_id={run_id}")
                return True
            else:
                logger.warning(f"Run not found: {run_id}")
                return False

        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error updating run metrics: {e}")
            return False

    def complete_run(
        self,
        run_id: int,
        status: str,
        metrics: Dict[str, Any],
        errors: Optional[List[str]] = None
    ) -> bool:
        """
        Mark a run as complete with final status, metrics, and errors.

        Args:
            run_id: Run log ID
            status: Final status (success, partial, failed - run_status enum)
            metrics: Dict with final metric values
            errors: List of error messages encountered during run

        Returns:
            True if successful, False otherwise
        """
        if not self._ensure_connection():
            return False

        try:
            # Build SET clause for metrics
            set_clauses = ["completed_at = %s", "status = %s::run_status", "errors = %s"]
            params = [datetime.utcnow(), status]

            errors_json = Json(errors) if errors else Json([])
            params.append(errors_json)

            # Add metric columns
            valid_metrics = [
                "total_shipments_read", "new_shipments", "shipments_checked",
                "shipments_updated", "shipments_delivered", "claims_created",
                "reports_generated", "reports_sent", "alerts_sent"
            ]

            for key, value in metrics.items():
                if key in valid_metrics and value is not None:
                    set_clauses.append(f"{key} = %s")
                    params.append(value)

            params.append(run_id)

            query = f"""
            UPDATE daily_run_logs
            SET {', '.join(set_clauses)}
            WHERE id = %s
            """

            self.cursor.execute(query, params)
            self.conn.commit()

            if self.cursor.rowcount > 0:
                logger.info(f"Run completed: run_id={run_id}, status={status}")
                return True
            else:
                logger.warning(f"Run not found: {run_id}")
                return False

        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error completing run: {e}")
            return False

    # ========== TENANT MAPPING & SPREADSHEET SYNC ==========

    def sync_tenant_mapping_from_spreadsheet(
        self,
        tenant_names,
        tenant_contacts
    ):
        """
        Synchronize tenant mapping from spreadsheet data using UPSERT logic.

        Args:
            tenant_names: {tenant_id_int: "CLIENT NAME", ...}
            tenant_contacts: {tenant_id_int: [{"name": "...", "whatsapp": "..."}, ...], ...}

        Returns:
            True if successful, False otherwise
        """
        if not self._ensure_connection():
            return False

        try:
            for tenant_id, client_name in tenant_names.items():
                whatsapp_numbers = []
                if tenant_id in tenant_contacts:
                    for contact in tenant_contacts[tenant_id]:
                        if "whatsapp" in contact and contact["whatsapp"]:
                            whatsapp_numbers.append(contact["whatsapp"])

                query = """
                INSERT INTO tenant_mapping (
                    dynamo_tenant_id, client_name, whatsapp_numbers, is_active
                )
                VALUES (%s, %s, %s, TRUE)
                ON CONFLICT (dynamo_tenant_id) DO UPDATE SET
                    client_name = EXCLUDED.client_name,
                    whatsapp_numbers = EXCLUDED.whatsapp_numbers,
                    is_active = TRUE,
                    updated_at = NOW()
                """

                whatsapp_json = Json(whatsapp_numbers)

                self.cursor.execute(query, (
                    tenant_id,
                    client_name,
                    whatsapp_json
                ))

            self.conn.commit()
            logger.info(f"Synced tenant mapping for {len(tenant_names)} tenants from spreadsheet")
            return True

        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error syncing tenant mapping from spreadsheet: {e}")
            return False

    # ========== SHIPMENT TRACKING DELIVERY CACHE ==========

    def get_delivered_tracking_set(self):
        """
        Get a set of all tracking numbers that have been marked as delivered.
        Used for caching delivered shipments.

        Returns:
            Set of tracking_number strings
        """
        if not self._ensure_connection():
            return set()

        try:
            query = """
            SELECT tracking_number FROM shipments
            WHERE is_delivered = TRUE
            """

            self.cursor.execute(query)
            results = self.cursor.fetchall()

            tracking_set = {row["tracking_number"] for row in results if row["tracking_number"]}
            logger.debug(f"Retrieved {len(tracking_set)} delivered tracking numbers")
            return tracking_set

        except psycopg2.Error as e:
            logger.error(f"Error getting delivered tracking set: {e}")
            return set()

    def mark_tracking_delivered(self, tracking_number):
        """
        Mark a shipment as delivered by tracking number.
        If the tracking number doesn't exist, insert a new minimal record.
        Uses UPSERT pattern.

        Args:
            tracking_number: FedEx tracking number

        Returns:
            True if successful, False otherwise
        """
        if not self._ensure_connection():
            return False

        try:
            query = """
            INSERT INTO shipments (tracking_number, is_delivered)
            VALUES (%s, TRUE)
            ON CONFLICT (tracking_number) DO UPDATE SET
                is_delivered = TRUE,
                updated_at = NOW()
            """

            self.cursor.execute(query, (tracking_number,))
            self.conn.commit()

            logger.debug(f"Marked tracking as delivered: {tracking_number}")
            return True

        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error marking tracking delivered: {e}")
            return False

    # ========== TABLE CREATION & SCHEMA INITIALIZATION ==========

    def ensure_tables_exist(self):
        """
        Ensure that required tables exist with their basic schema.
        Creates tenant_mapping and shipments tables if they don't exist.

        Returns:
            True if all tables exist or were created successfully, False otherwise
        """
        if not self._ensure_connection():
            return False

        try:
            tenant_mapping_query = """
            CREATE TABLE IF NOT EXISTS tenant_mapping (
                dynamo_tenant_id INTEGER PRIMARY KEY,
                client_id INTEGER,
                client_name VARCHAR(255),
                odoo_company_id INTEGER,
                whatsapp_numbers JSONB DEFAULT '[]',
                is_active BOOLEAN DEFAULT TRUE,
                notes TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
            """

            self.cursor.execute(tenant_mapping_query)
            logger.debug("Ensured tenant_mapping table exists")

            shipments_query = """
            CREATE TABLE IF NOT EXISTS shipments (
                id SERIAL PRIMARY KEY,
                tracking_number VARCHAR(255) UNIQUE NOT NULL,
                is_delivered BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
            """

            self.cursor.execute(shipments_query)
            logger.debug("Ensured shipments table exists")

            self.conn.commit()
            logger.info("All required tables verified/created successfully")
            return True

        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error ensuring tables exist: {e}")
            return False

    # ========================================================================
    # ALIAS METHODS for main.py v1.2.0 compatibility
    # ========================================================================

    def get_client_by_tenant(self, dynamo_tenant_id: int) -> Optional[Dict[str, Any]]:
        """Get tenant mapping for a specific dynamo tenant ID."""
        if not self._ensure_connection():
            return None
        try:
            self.cursor.execute(
                "SELECT * FROM tenant_mapping WHERE dynamo_tenant_id = %s",
                (dynamo_tenant_id,)
            )
            row = self.cursor.fetchone()
            if row:
                cols = [desc[0] for desc in self.cursor.description]
                return dict(row)
            return None
        except Exception as e:
            logger.error(f"get_client_by_tenant error: {e}")
            return None

    def create_proactive_claim(self, tracking_number, shipment_id=None,
                                client_id=None, claim_type='',
                                description='', rule=''):
        """Create a proactive claim from anomaly detection."""
        return self.create_claim({
            "tracking_number": tracking_number,
            "shipment_id": shipment_id,
            "client_id": client_id,
            "claim_type": claim_type,
            "description": description,
            "origin": "automatico",
            "created_automatically": True,
            "auto_detection_rule": rule,
            "status": "nuevo",
        })

    def create_run_log(self, run_date):
        """Alias for start_run."""
        return self.start_run(run_date)
    def update_run_log(self, run_id, stats, errors, status):
        """Update run log with stats, errors, and status."""
        if not self._ensure_connection():
            return False
        try:
            import json as _json
            self.cursor.execute(
                "UPDATE daily_run_logs SET"
                " total_shipments_read=%s, new_shipments=%s,"
                " status_updates=%s, anomalies_detected=%s,"
                " claims_created=%s, reports_sent=%s,"
                " errors=%s, status=%s::run_status,"
                " finished_at=NOW() WHERE id=%s",
                (
                    stats.get("total_shipments", 0),
                    stats.get("new_shipments", 0),
                    stats.get("shipments_updated", 0),
                    stats.get("anomalies_detected", 0),
                    stats.get("claims_created", 0),
                    stats.get("reports_sent", 0),
                    _json.dumps(errors) if errors else None,
                    status, run_id,
                )
            )
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"update_run_log error: {e}")
            if self.conn:
                self.conn.rollback()
            return False

    def update_shipment_from_fedex(self, tracking_number, fedex_data):
        """Update shipment with FedEx data."""
        return self.update_shipment_fedex_data(tracking_number, fedex_data)

