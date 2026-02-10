"""
SonIA Core — DynamoDB Reader Module
READ ONLY access to DynamoDB reserves table.

⚠️ CRITICAL: This module ONLY performs read operations.
No writes, modifications, or deletions are allowed.
"""

import logging
import boto3
from botocore.config import Config as BotoConfig
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class DynamoReader:
    """Read-only client for DynamoDB reserves table."""

    def __init__(self, aws_access_key: str, aws_secret_key: str,
                 region: str = "us-east-1", table_name: str = "reserves"):
        self.table_name = table_name

        boto_config = BotoConfig(
            region_name=region,
            retries={"max_attempts": 3, "mode": "adaptive"}
        )

        self.client = boto3.client(
            "dynamodb",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            config=boto_config,
        )
        logger.info(f"DynamoDB reader initialized for table '{table_name}' in {region}")

    def scan_all_reserves(self) -> List[Dict[str, Any]]:
        """
        Scan all items from the reserves table.
        Returns a list of parsed reserve objects.

        ⚠️ READ ONLY operation (dynamodb:Scan)
        """
        logger.info(f"Starting full scan of '{self.table_name}'...")

        items = []
        scan_kwargs = {"TableName": self.table_name}

        while True:
            response = self.client.scan(**scan_kwargs)
            raw_items = response.get("Items", [])

            for raw in raw_items:
                parsed = self._parse_reserve(raw)
                if parsed:
                    items.append(parsed)

            # Check for pagination
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break
            scan_kwargs["ExclusiveStartKey"] = last_key

        logger.info(f"Scan complete: {len(items)} reserves found")
        return items

    def _parse_reserve(self, raw: Dict) -> Optional[Dict[str, Any]]:
        """Parse a raw DynamoDB item into a clean reserve dict."""
        try:
            reserve = {
                "id": self._get_s(raw, "id"),
                "tenant": self._get_n(raw, "tenant"),
                "order_id": self._get_s(raw, "orderId"),
                "order_number": self._get_s(raw, "orderNumber"),
                "ecommerce_order_id": self._get_s(raw, "ecommerceOrderId"),
                "ecommerce_id": self._get_n(raw, "ecommerceId"),
                "shipping_postal_code": self._get_s(raw, "shippingAddressPostalCode"),
                "carrier_report_id": self._get_s(raw, "carrierReportId"),
                "created_at": self._get_s(raw, "createdAt"),
                "updated_at": self._get_s(raw, "updatedAt"),
                "packages": [],
            }

            # Parse packages list
            packages_raw = raw.get("packages", {}).get("L", [])
            for pkg_raw in packages_raw:
                pkg_map = pkg_raw.get("M", {})
                package = self._parse_package(pkg_map)
                if package and package.get("tracking_number"):
                    reserve["packages"].append(package)

            return reserve
        except Exception as e:
            logger.error(f"Error parsing reserve: {e}")
            return None

    def _parse_package(self, pkg_map: Dict) -> Optional[Dict[str, Any]]:
        """Parse a package map from DynamoDB."""
        try:
            return {
                "id": self._get_s(pkg_map, "id"),
                "tracking_number": self._get_s(pkg_map, "trackingNumber"),
                "status": self._get_s(pkg_map, "status"),
                "status_date": self._get_n(pkg_map, "statusDate"),
                "gross_weight": self._get_n_float(pkg_map, "grossWeight"),
                "piece_id": self._get_s(pkg_map, "pieceId"),
                "carrier_tracking_data": self._get_s(pkg_map, "carrierTrackingData"),
            }
        except Exception as e:
            logger.error(f"Error parsing package: {e}")
            return None

    @staticmethod
    def _get_s(item: Dict, key: str) -> Optional[str]:
        """Get string value from DynamoDB item."""
        val = item.get(key, {}).get("S")
        return val if val else None

    @staticmethod
    def _get_n(item: Dict, key: str) -> Optional[int]:
        """Get number value (as int) from DynamoDB item."""
        val = item.get(key, {}).get("N")
        return int(val) if val else None

    @staticmethod
    def _get_n_float(item: Dict, key: str) -> Optional[float]:
        """Get number value (as float) from DynamoDB item."""
        val = item.get(key, {}).get("N")
        return float(val) if val else None

    def extract_all_tracking_numbers(self, reserves: List[Dict]) -> List[Dict[str, Any]]:
        """
        From a list of reserves, extract all packages with their tracking numbers,
        grouped with their tenant/client info.

        Returns list of dicts:
        {
            "tracking_number": "...",
            "tenant": 1,
            "reserve_id": "...",
            "order_id": "...",
            "package_id": "...",
            "dynamo_status": "...",
            "dynamo_status_date": ...,
        }
        """
        tracking_list = []

        for reserve in reserves:
            tenant = reserve.get("tenant")
            reserve_id = reserve.get("id")
            order_id = reserve.get("order_id")

            for pkg in reserve.get("packages", []):
                tn = pkg.get("tracking_number")
                if tn:
                    tracking_list.append({
                        "tracking_number": tn,
                        "tenant": tenant,
                        "reserve_id": reserve_id,
                        "order_id": order_id,
                        "package_id": pkg.get("id"),
                        "dynamo_status": pkg.get("status"),
                        "dynamo_status_date": pkg.get("status_date"),
                        "gross_weight": pkg.get("gross_weight"),
                    })

        logger.info(f"Extracted {len(tracking_list)} tracking numbers from {len(reserves)} reserves")
        return tracking_list
