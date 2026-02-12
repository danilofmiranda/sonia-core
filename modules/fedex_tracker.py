"""
SonIA Core â FedEx Tracking Module
Handles OAuth2 authentication, batch tracking, and status normalization.
Uses connection pooling with httpx and exponential backoff retry logic.
"""

import logging
import httpx
import json
import time
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def get_sonia_status(status_code: str, description: str = "") -> str:
    """
    Convert FedEx status code and description to SonIA DB-compatible status.

    Checks description FIRST (priority), then falls back to status_code.
    All output is lowercase_with_underscores for DB enum compatibility.

    Valid DB enum values:
    'label_created', 'picked_up', 'in_transit', 'in_customs', 'out_for_delivery',
    'delivered', 'exception', 'delayed', 'on_hold', 'delivery_attempted',
    'returned_to_sender', 'cancelled', 'unknown'
    """

    # PRIORITY 1: Check description first (more reliable than status codes)
    desc_lower = description.lower() if description else ""

    # Label created
    if any(term in desc_lower for term in ["shipment information sent", "label created", "shipping label"]):
        return "label_created"

    # Delivered
    if "delivered" in desc_lower:
        return "delivered"

    # Out for delivery
    if any(term in desc_lower for term in ["out for delivery", "on fedex vehicle for delivery"]):
        return "out_for_delivery"

    # Picked up
    if any(term in desc_lower for term in ["picked up", "package received"]):
        return "picked_up"

    # In transit
    if any(term in desc_lower for term in ["in transit", "departed", "arrived", "left fedex", "at fedex", "on the way", "at destination sort", "at local fedex", "in fedex", "international shipment release"]):
        return "in_transit"

    # In customs
    if any(term in desc_lower for term in ["clearance", "customs", "import", "broker"]):
        return "in_customs"

    # Exception
    if "exception" in desc_lower:
        return "exception"

    # Delayed
    if "delay" in desc_lower:
        return "delayed"

    # On hold
    if "hold" in desc_lower:
        return "on_hold"

    # Delivery attempted
    if any(term in desc_lower for term in ["delivery attempt", "unable to deliver"]):
        return "delivery_attempted"

    # Returned to sender
    if "return" in desc_lower:
        return "returned_to_sender"

    # Cancelled
    if "cancel" in desc_lower:
        return "cancelled"

    # PRIORITY 2: Fall back to status_code if no description match
    code_upper = status_code.upper() if status_code else ""

    # Delivered
    if code_upper == "DL":
        return "delivered"

    # Out for delivery
    if code_upper == "OD":
        return "out_for_delivery"

    # Picked up
    if code_upper == "PU":
        return "picked_up"

    # In transit
    if code_upper in ["IT", "AA", "AR", "DP", "AF", "PM"]:
        return "in_transit"

    # Exception
    if code_upper in ["DE", "SE", "OC"]:
        return "exception"

    # On hold
    if code_upper == "HL":
        return "on_hold"

    # Returned to sender
    if code_upper == "RS":
        return "returned_to_sender"

    # Cancelled
    if code_upper == "CA":
        return "cancelled"

    # In customs
    if code_upper == "CD":
        return "in_customs"

    # Label created
    if code_upper in ["IN", "SP", "PL"]:
        return "label_created"

    # Default
    return "unknown"


class FedExTracker:
    def __init__(self, client_id, client_secret, account_number, sandbox=False):
        self.client_id = client_id
        self.client_secret = client_secret
        self.account_number = account_number
        self.sandbox = sandbox
        if sandbox:
            self.auth_url = "https://apis-sandbox.fedex.com/oauth/authorize"
            self.token_url = "https://apis-sandbox.fedex.com/oauth/token"
            self.track_url = "https://apis-sandbox.fedex.com/track/v1/trackingnumbers"
        else:
            self.auth_url = "https://apis.fedex.com/oauth/authorize"
            self.token_url = "https://apis.fedex.com/oauth/token"
            self.track_url = "https://apis.fedex.com/track/v1/trackingnumbers"
        self.access_token = None
        self.token_expires_at = None
        self.client = httpx.Client(
            timeout=30.0,
            limits=httpx.Limits(max_connections=5, max_keepalive_connections=3)
        )
        logger.info(f"FedExTracker initialized ({'sandbox' if sandbox else 'production'})")

    def authenticate(self):
        try:
            logger.info("Authenticating with FedEx OAuth2...")
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            data = {"grant_type": "client_credentials", "client_id": self.client_id, "client_secret": self.client_secret}
            response = self._request_with_retry("POST", self.token_url, headers=headers, data=data)
            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data.get("access_token")
                expires_in = token_data.get("expires_in", 3600)
                self.token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in - 60)
                logger.info(f"Authentication successful. Token expires at {self.token_expires_at}")
                return True
            else:
                logger.error(f"Authentication failed: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return False

    def track_batch(self, tracking_numbers):
        if not tracking_numbers:
            return {}
        if not self._is_token_valid():
            if not self.authenticate():
                logger.error("Failed to authenticate for tracking")
                return {tn: {"error": "Authentication failed"} for tn in tracking_numbers}
        results = {}
        batch_size = 30
        for i in range(0, len(tracking_numbers), batch_size):
            batch = tracking_numbers[i:i + batch_size]
            batch_results = self._track_batch_request(batch)
            results.update(batch_results)
        return results

    def _track_batch_request(self, tracking_numbers):
        try:
            headers = {"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"}
            payload = {
                "trackingInfo": [{"trackingNumberInfo": {"trackingNumber": tn}} for tn in tracking_numbers],
                "includeDetailedScans": True
            }
            logger.info(f"Tracking batch of {len(tracking_numbers)} packages via POST")
            response = self._request_with_retry("POST", self.track_url, headers=headers, json_data=payload)
            results = {}
            if response.status_code == 200:
                data = response.json()
                tracking_results = data.get("output", {}).get("completeTrackResults", [])
                for result in tracking_results:
                    tn = result.get("trackingNumber")
                    if tn:
                        parsed = self._parse_tracking_result(result)
                        results[tn] = parsed
            else:
                logger.warning(f"Tracking request failed: {response.status_code} - {response.text[:500]}")
                for tn in tracking_numbers:
                    results[tn] = {"error": f"API returned {response.status_code}", "raw_response": response.text[:1000]}
            return results
        except Exception as e:
            logger.error(f"Error in batch tracking request: {e}")
            return {tn: {"error": str(e)} for tn in tracking_numbers}

    def _parse_tracking_result(self, result):
        """
        Parse a FedEx Track API v1 tracking result into normalized format.
        `result` is a completeTrackResults item with trackResults array inside.

        Extracts all fields needed for Excel report and DB storage, matching
        SonIA Tracker's parse_tracking_response() logic exactly.
        """
        try:
            parsed = {
                "raw_fedex_response": result
            }

            # FedEx Track API v1 nests results in trackResults array
            track_results = result.get("trackResults", [])
            if not track_results:
                return {
                    "sonia_status": "unknown",
                    "fedex_status": "No track results",
                    "is_delivered": False,
                    "raw_fedex_response": result,
                }

            track_detail = track_results[0]

            # Extract status from latestStatusDetail
            latest_status = track_detail.get("latestStatusDetail", {})
            status_code = latest_status.get("code", "")
            status_description = latest_status.get("description", "")

            # Get normalized SonIA status
            sonia_status = get_sonia_status(status_code, status_description)
            parsed["sonia_status"] = sonia_status
            parsed["fedex_status"] = status_description
            parsed["fedex_status_code"] = status_code

            # Extract estimated delivery from dateAndTimes
            estimated_delivery = None
            date_and_times = track_detail.get("dateAndTimes", [])
            for dt in date_and_times:
                if dt.get("type") in ["ESTIMATED_DELIVERY", "ESTIMATED_DELIVERY_TIMESTAMP"]:
                    estimated_delivery = dt.get("dateTime")
                    break
            if estimated_delivery:
                parsed["estimated_delivery_date"] = estimated_delivery[:10] if len(estimated_delivery) >= 10 else estimated_delivery
            else:
                parsed["estimated_delivery_date"] = None

            # Extract scan events
            scan_events_list = track_detail.get("scanEvents", [])
            parsed["scan_events"] = []
            latest_event = None

            if scan_events_list:
                # Process first 5 scan events
                for i, event in enumerate(scan_events_list[:5]):
                    event_date = event.get("date", "")
                    event_desc = event.get("eventDescription", "")
                    scan_location = event.get("scanLocation", {})
                    event_city = scan_location.get("city", "")

                    scan_event_dict = {
                        "date": event_date,
                        "description": event_desc,
                        "city": event_city
                    }
                    parsed["scan_events"].append(scan_event_dict)

                    # Capture latest event (first in list)
                    if i == 0:
                        latest_event = scan_event_dict

            parsed["latest_event"] = latest_event

            # Extract label_creation_date from scan_events in reverse order
            label_creation_date = None
            if scan_events_list:
                for event in reversed(scan_events_list):
                    event_desc = event.get("eventDescription", "").lower()
                    if any(term in event_desc for term in ["shipment information sent", "label created", "shipping label"]):
                        event_date = event.get("date", "")
                        if event_date:
                            label_creation_date = event_date[:10]
                            break
            parsed["label_creation_date"] = label_creation_date

            # Extract ship_date from dateAndTimes or scan_events
            ship_date = None

            # First check dateAndTimes for ACTUAL_PICKUP or SHIP
            for date_time_entry in date_and_times:
                date_type = date_time_entry.get("type", "")
                if date_type in ["ACTUAL_PICKUP", "SHIP"]:
                    date_val = date_time_entry.get("dateTime", "")
                    if date_val:
                        ship_date = date_val[:10]
                        break

            # Fallback: search scan_events in reverse for "picked up" or "package received"
            if not ship_date and scan_events_list:
                for event in reversed(scan_events_list):
                    event_desc = event.get("eventDescription", "").lower()
                    if any(term in event_desc for term in ["picked up", "package received"]):
                        event_date = event.get("date", "")
                        if event_date:
                            ship_date = event_date[:10]
                            break

            parsed["ship_date"] = ship_date

            # Extract delivery_date from dateAndTimes ACTUAL_DELIVERY
            delivery_date = None
            for date_time_entry in date_and_times:
                date_type = date_time_entry.get("type", "")
                if date_type == "ACTUAL_DELIVERY":
                    date_val = date_time_entry.get("dateTime", "")
                    if date_val:
                        delivery_date = date_val[:10]
                        break

            parsed["delivery_date"] = delivery_date

            # Extract destination address information
            destination_city = None
            destination_state = None
            destination_country = None

            # Try recipientInformation first
            recipient_info = track_detail.get("recipientInformation", {})
            recipient_address = recipient_info.get("address", {})

            if recipient_address:
                destination_city = recipient_address.get("city")
                destination_state = recipient_address.get("stateOrProvinceCode")
                destination_country = recipient_address.get("countryCode")

            # Fallback to destinationLocation
            if not destination_city:
                dest_location = track_detail.get("destinationLocation", {})
                dest_address = dest_location.get("locationContactAndAddress", {}).get("address", {})
                destination_city = dest_address.get("city")
                destination_state = dest_address.get("stateOrProvinceCode")
                destination_country = dest_address.get("countryCode")

            parsed["destination_city"] = destination_city
            parsed["destination_state"] = destination_state
            parsed["destination_country"] = destination_country

            # Set is_delivered
            is_delivered = sonia_status == "delivered" or delivery_date is not None
            parsed["is_delivered"] = is_delivered

            logger.debug(f"Parsed tracking result: {parsed}")
            return parsed

        except Exception as e:
            logger.error(f"Error parsing tracking result: {e}")
            return {
                "error": str(e),
                "raw_fedex_response": result,
                "sonia_status": "unknown",
                "is_delivered": False
            }

    def _is_token_valid(self):
        if not self.access_token or not self.token_expires_at:
            return False
        return datetime.utcnow() < self.token_expires_at

    def _request_with_retry(self, method, url, headers=None, data=None, json_data=None, max_retries=3):
        for attempt in range(max_retries):
            try:
                if json_data:
                    response = self.client.request(method, url, headers=headers, json=json_data)
                else:
                    response = self.client.request(method, url, headers=headers, data=data)
                if response.status_code in [429, 500, 502, 503, 504]:
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        logger.warning(f"Request failed with {response.status_code}, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                return response
            except httpx.RequestError as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Request error: {e}, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                raise
        return response

    def track_multiple(self, tracking_numbers):
        return self.track_batch(tracking_numbers)

    def close(self):
        if self.client:
            self.client.close()
            logger.info("FedExTracker client closed")
