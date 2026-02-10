"""
SonIA Core â Anomaly Detector Module (Part C)
Detects shipment anomalies and creates proactive claims.

Detection Rules:
1. exception_detected - FedEx reported an exception
2. transit_too_long - Package in transit > X business days
3. returned_to_sender - Package returned to origin
4. delivery_attempted_stuck - Failed delivery attempt > X days
5. customs_too_long - Package stuck in customs > X days
6. label_no_movement - Label created but no pickup > X days
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

COT = timezone(timedelta(hours=-5))


class AnomalyDetector:
    """Detects shipping anomalies and recommends claim creation."""

    def __init__(self, thresholds: Dict[str, int] = None):
        """
        Initialize with configurable thresholds.

        Args:
            thresholds: Dict with keys like 'transit_days', 'customs_days', etc.
        """
        self.thresholds = thresholds or {
            "transit_days": 7,
            "customs_days": 5,
            "delivery_attempt_days": 2,
            "label_no_movement_days": 5,
        }
        logger.info(f"Anomaly detector initialized with thresholds: {self.thresholds}")

    def check_shipment(self, shipment: Dict) -> List[Dict[str, Any]]:
        """
        Check a single shipment for anomalies.

        Args:
            shipment: Dict with keys: tracking_number, sonia_status, ship_date,
                      label_creation_date, last_status_change, client_name, etc.

        Returns:
            List of detected anomalies, each a dict with:
            {
                "rule": "rule_name",
                "tracking_number": "...",
                "claim_type": "...",  # Maps to claim_type enum
                "description": "...",
                "severity": "high" | "medium" | "low"
            }
        """
        anomalies = []

        status = (shipment.get("sonia_status") or "unknown").lower()
        tracking = shipment.get("tracking_number", "N/A")

        # Skip delivered shipments
        if status == "delivered" or shipment.get("is_delivered"):
            return anomalies

        now = datetime.now(COT)

        # Rule 1: Exception detected
        if status == "exception":
            anomalies.append({
                "rule": "exception_detected",
                "tracking_number": tracking,
                "claim_type": "otro",
                "description": f"FedEx reportÃ³ una excepciÃ³n de entrega. Estado: {shipment.get('fedex_status', 'N/A')}",
                "severity": "high",
            })

        # Rule 2: Transit too long
        if status == "in_transit":
            ship_date = self._parse_date(shipment.get("ship_date"))
            if ship_date:
                business_days = self._count_business_days(ship_date, now)
                threshold = self.thresholds.get("transit_days", 7)
                if business_days > threshold:
                    anomalies.append({
                        "rule": "transit_too_long",
                        "tracking_number": tracking,
                        "claim_type": "entrega_tardia",
                        "description": f"Paquete en trÃ¡nsito por {business_days} dÃ­as hÃ¡biles (umbral: {threshold}). Enviado: {ship_date.strftime('%Y-%m-%d')}",
                        "severity": "medium",
                    })

        # Rule 3: Returned to sender
        if status == "returned_to_sender":
            anomalies.append({
                "rule": "returned_to_sender",
                "tracking_number": tracking,
                "claim_type": "no_entregado",
                "description": f"Paquete devuelto a origen. Estado FedEx: {shipment.get('fedex_status', 'N/A')}",
                "severity": "high",
            })

        # Rule 4: Delivery attempted but stuck
        if status == "delivery_attempted":
            last_change = self._parse_date(shipment.get("last_status_change"))
            if last_change:
                days_stuck = (now - last_change).days
                threshold = self.thresholds.get("delivery_attempt_days", 2)
                if days_stuck > threshold:
                    anomalies.append({
                        "rule": "delivery_attempted_stuck",
                        "tracking_number": tracking,
                        "claim_type": "no_entregado",
                        "description": f"Intento de entrega sin Ã©xito por {days_stuck} dÃ­as (umbral: {threshold})",
                        "severity": "medium",
                    })

        # Rule 5: Customs too long
        if status == "in_customs":
            last_change = self._parse_date(shipment.get("last_status_change"))
            if last_change:
                business_days = self._count_business_days(last_change, now)
                threshold = self.thresholds.get("customs_days", 5)
                if business_days > threshold:
                    anomalies.append({
                        "rule": "customs_too_long",
                        "tracking_number": tracking,
                        "claim_type": "entrega_tardia",
                        "description": f"Paquete en aduanas por {business_days} dÃ­as hÃ¡biles (umbral: {threshold})",
                        "severity": "medium",
                    })

        # Rule 6: Label created but no movement
        if status == "label_created":
            label_date = self._parse_date(shipment.get("label_creation_date"))
            if label_date:
                days_since = (now - label_date).days
                threshold = self.thresholds.get("label_no_movement_days", 5)
                if days_since > threshold:
                    anomalies.append({
                        "rule": "label_no_movement",
                        "tracking_number": tracking,
                        "claim_type": "otro",
                        "description": f"Label creada hace {days_since} dÃ­as sin movimiento (umbral: {threshold}). Label: {label_date.strftime('%Y-%m-%d')}",
                        "severity": "low",
                    })

        return anomalies

    def check_all_shipments(self, shipments: List[Dict]) -> List[Dict[str, Any]]:
        """
        Check all shipments for anomalies.
        Returns a flat list of all detected anomalies.
        """
        all_anomalies = []

        for shipment in shipments:
            anomalies = self.check_shipment(shipment)
            for a in anomalies:
                a["client_id"] = shipment.get("client_id")
                a["client_name"] = shipment.get("client_name", "")
                a["shipment_id"] = shipment.get("id")
            all_anomalies.extend(anomalies)

        if all_anomalies:
            by_rule = {}
            for a in all_anomalies:
                rule = a["rule"]
                by_rule[rule] = by_rule.get(rule, 0) + 1
            logger.warning(f"Detected {len(all_anomalies)} anomalies: {by_rule}")
        else:
            logger.info("No anomalies detected")

        return all_anomalies

    @staticmethod
    def _parse_date(date_val) -> Optional[datetime]:
        """Parse various date formats into datetime with timezone."""
        if not date_val:
            return None

        if isinstance(date_val, datetime):
            if date_val.tzinfo is None:
                return date_val.replace(tzinfo=COT)
            return date_val

        if isinstance(date_val, str):
            # Try common formats
            for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"]:
                try:
                    dt = datetime.strptime(date_val, fmt)
                    return dt.replace(tzinfo=COT)
                except ValueError:
                    continue

        return None

    @staticmethod
    def _count_business_days(start: datetime, end: datetime) -> int:
        """Count business days (Mon-Fri) between two dates."""
        if start.tzinfo and end.tzinfo is None:
            end = end.replace(tzinfo=start.tzinfo)

        count = 0
        current = start.date() if hasattr(start, 'date') else start
        end_date = end.date() if hasattr(end, 'date') else end

        while current < end_date:
            if current.weekday() < 5:  # Monday=0 to Friday=4
                count += 1
            current += timedelta(days=1)

        return count
