"""
SonIA Core — FedEx Tracking Module
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
    Normalize FedEx status codes to SonIA standard status format.

    FedEx status codes are normalized to lowercase with underscores:
    - label_created
    - picked_up
    - in_transit
    - in_customs
    - out_for_delivery
    - delivered
    - exception
    - delayed
    - on_hold
    - delivery_attempted
    - returned_to_sender
    - cancelled
    - unknown

    Args:
        status_code: FedEx status code or description
        description: Additional status description from FedEx

    Returns:
        Normalized status string in lowercase_with_underscores format
    """
    if not status_code:
        return "unknown"

    status_upper = status_code.upper().strip()
    desc_upper = (description or "").upper().strip()

    # Delivery status
    if "DELIVERED" in status_upper or "DL" in status_upper:
        return "delivered"

    # In Transit / On Way
    if any(x in status_upper for x in ["IN TRANSIT", "IN_TRANSIT", "IT", "ON THE WAY"]):
        return "in_transit"

    # Out for Delivery
    if any(x in status_upper for x in ["OUT FOR DELIVERY", "OUT_FOR_DELIVERY", "OD"]):
        return "out_for_delivery"

    # Picked Up
    if any(x in status_upper for x in ["PICKED UP", "PICKED_UP", "PU"]):
        return "picked_up"

    # Label Created / On FedEx
    if any(x in status_upper for x in ["LABEL", "ON FEDEX", "OF"]):
        return "label_created"

    # Customs
    if any(x in status_upper for x in ["CUSTOMS", "CLEARANCE"]):
        return "in_customs"

    # On Hold
    if any(x in status_upper for x in ["ON HOLD", "HELD", "HOLD"]):
        return "on_hold"

    # Delayed
    if "DELAYED" in status_upper or "DELAY" in status_upper:
        return "delayed"

    # Exception / Problem
    if any(x in status_upper for x in ["EXCEPTION", "PROBLEM", "FAILURE", "RETURNED"]):
        if "RETURNED" in status_upper:
            return "returned_to_sender"
        return "exception"

    # Delivery Attempted
    if any(x in status_upper for x in ["ATTEMPTED", "ATTEMPT"]):
        return "delivery_attempted"

    # Returned to Sender
    if "RETURNED" in status_upper or "RETURN" in status_upper:
        return "returned_to_sender"

    # Cancelled
    if "CANCEL" in status_upper:
        return "cancelled"

    return "unknown"


class FedExTracker:
    """
    FedEx tracking client with OAuth2 authentication, batch tracking, and retry logic.
    Uses httpx with connection pooling for improved performance.
    """

    def __init__(self, client_id: str, client_secret: str, account_number: str,
                 sandbox: bool = False):
        """
        Initialize FedEx tracker with OAuth2 credentials.

        Args:
            client_id: FedEx OAuth2 client ID
            client_secret: FedEx OAuth2 client secret
            account_number: FedEx account number for tracking
            sandbox: Use sandbox environment if True
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.account_number = account_number
        self.sandbox = sandbox

        # API endpoints
        if sandbox:
            self.auth_url = "https://apis-sandbox.fedex.com/oauth/authorize"
            self.token_url = "https://apis-sandbox.fedex.com/oauth/token"
            self.track_url = "https://apis-sandbox.fedex.com/track/v1/trackingnumbers"
        else:
            self.auth_url = "https://apis.fedex.com/oauth/authorize"
            self.token_url = "https://apis.fedex.com/oauth/token"
            self.track_url = "https://apis.fedex.com/track/v1/trackingnumbers"

        # Token management
        self.access_token: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None

        # HTTP client with connection pooling
        self.client = httpx.Client(
            timeout=30.0,
            limits=httpx.Limits(max_connections=5, max_keepalive_connections=3)
        )

        logger.info(f"FedExTracker initialized ({'sandbox' if sandbox else 'production'})")

    def authenticate(self) -> bool:
        """
        Authenticate with FedEx OAuth2 and cache the token.

        Returns:
            True if authentication successful, False otherwise
        """
        try:
            logger.info("Authenticating with FedEx OAuth2...")

            # Prepare auth request - FedEx expects credentials in POST body
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
            }

            data = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }

            # Request token with retry logic
            response = self._request_with_retry(
                "POST",
                self.token_url,
                headers=headers,
                data=data
            )

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

    def track_batch(self, tracking_numbers: List[str]) -> Dict[str, Any]:
        """
        Track a batch of FedEx shipments (up to 30 per request).

        Args:
            tracking_numbers: List of FedEx tracking numbers

        Returns:
            Dict mapping tracking_number -> {
                "status": "...",
                "status_detail": "...",
                "estimated_delivery": "...",
                "latest_event": {...},
                "raw_response": {...},
                "error": "..." (if applicable)
            }
        """
        if not tracking_numbers:
            return {}

        # Ensure token is fresh
        if not self._is_token_valid():
            if not self.authenticate():
                logger.error("Failed to authenticate for tracking")
                return {tn: {"error": "Authentication failed"} for tn in tracking_numbers}

        results = {}
        batch_size = 30

        # Process in batches of 30
        for i in range(0, len(tracking_numbers), batch_size):
            batch = tracking_numbers[i:i + batch_size]
            batch_results = self._track_batch_request(batch)
            results.update(batch_results)

        return results

    def _track_batch_request(self, tracking_numbers: List[str]) -> Dict[str, Any]:
        """Make a single batch tracking request to FedEx Track API v1 (POST)."""
        try:
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
            }

            # Build JSON payload per FedEx Track API v1 spec
            payload = {
                "trackingInfo": [
                    {"trackingNumberInfo": {"trackingNumber": tn}}
                    for tn in tracking_numbers
                ],
                "includeDetailedScans": True
            }

            logger.info(f"Tracking batch of {len(tracking_numbers)} packages via POST")

            response = self._request_with_retry(
                "POST", self.track_url, headers=headers, json_data=payload
            )

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
                    results[tn] = {
                        "error": f"API returned {response.status_code}",
                        "raw_response": response.text[:1000]
                    }

            return results

        except Exception as e:
            logger.error(f"Error in batch tracking request: {e}")
            return {tn: {"error": str(e)} for tn in tracking_numbers}

    def _parse_tracking_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a FedEx Track API v1 tracking result into normalized format.

        Args:
            result: Raw FedEx API completeTrackResults item

        Returns:
            Parsed result dict with normalized status
        """
        try:
            tn = result.get("trackingNumber")

            # FedEx Track API v1 nests results in trackResults array
            track_results = result.get("trackResults", [])
            if not track_results:
                return {
                    "sonia_status": "unknown",
                    "fedex_status": "No track results",
                    "estimated_delivery_date": None,
                    "is_delivered": False,
                    "latest_event": {},
                    "raw_fedex_response": result,
                }

            track_detail = track_results[0]

            # Extract status from latestStatusDetail
            latest_status = track_detail.get("latestStatusDetail", {})
            status_code = latest_status.get("code", "")
            status_description = latest_status.get("description", "")

            # Normalize status
            sonia_status = get_sonia_status(status_code, status_description)

            # Extract estimated delivery from dateAndTimes array
            estimated_delivery = None
            date_times = track_detail.get("dateAndTimes", [])
            for dt in date_times:
                if dt.get("type") in ["ESTIMATED_DELIVERY", "ESTIMATED_DELIVERY_TIMESTAMP"]:
                    estimated_delivery = dt.get("dateTime")
                    break

            # Extract latest scan event
            latest_event = {}
            scan_events = track_detail.get("scanEvents", [])
            if scan_events:
                event = scan_events[0]
                scan_location = event.get("scanLocation", {})
                latest_event = {
                    "date": event.get("date"),
                    "time": event.get("date"),
                    "location": {
                        "city": scan_location.get("city"),
                        "state": scan_location.get("stateOrProvinceCode"),
                        "country": scan_location.get("countryCode"),
                    },
                    "description": event.get("eventDescription"),
                }

            parsed = {
                "sonia_status": sonia_status,
                "fedex_status": status_description or latest_status.get("statusByLocale", ""),
                "estimated_delivery_date": estimated_delivery,
                "is_delivered": sonia_status == "delivered",
                "latest_event": latest_event,
                "raw_fedex_response": result,
            }

            return parsed

        except Exception as e:
            logger.error(f"Error parsing tracking result for {result.get('trackingNumber')}: {e}")
            return {
                "error": f"Parse error: {str(e)}",
                "raw_response": result
            }

    def _is_token_valid(self) -> bool:
        """Check if cached token is still valid."""
        if not self.access_token or not self.token_expires_at:
            return False
        return datetime.utcnow() < self.token_expires_at

    def _request_with_retry(self, method: str, url: str, headers: Dict = None,
                           data: Dict = None, json_data: Dict = None,
                           max_retries: int = 3) -> httpx.Response:
        """
        Make HTTP request with exponential backoff retry logic.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            headers: Request headers
            data: Form data
            json_data: JSON data
            max_retries: Maximum retry attempts

        Returns:
            httpx.Response object
        """
        for attempt in range(max_retries):
            try:
                if json_data:
                    response = self.client.request(
                        method, url, headers=headers, json=json_data
                    )
                else:
                    response = self.client.request(
                        method, url, headers=headers, data=data
                    )

                # Retry on 5xx errors or rate limiting (429)
                if response.status_code in [429, 500, 502, 503, 504]:
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
                        logger.warning(f"Request failed with {response.status_code}, "
                                     f"retrying in {wait_time}s...")
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


    def track_multiple(self, tracking_numbers: List[str]) -> Dict[str, Any]:
        """Alias for track_batch - tracks multiple FedEx shipments."""
        return self.track_batch(tracking_numbers)

    def close(self):
        """Close HTTP client and cleanup resources."""
        if self.client:
            self.client.close()
            logger.info("FedExTracker client closed")

