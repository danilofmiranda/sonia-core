"""
SonIA Core — WhatsApp Sender Module
Sends reports and alerts through SonIA Agent's API.
"""

import logging
import httpx
from typing import Optional

logger = logging.getLogger(__name__)


class WhatsAppSender:
    """Sends WhatsApp messages via SonIA Agent API."""

    def __init__(self, agent_url: str, api_key: str):
        self.agent_url = agent_url.rstrip("/")
        self.api_key = api_key
        self._client = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0),
                headers={"X-API-Key": self.api_key}
            )
        return self._client

    def send_message_sync(self, phone_number: str, message: str) -> bool:
        """Send a text message via WhatsApp (synchronous)."""
        try:
            with httpx.Client(timeout=30.0, headers={"X-API-Key": self.api_key}) as client:
                response = client.post(
                    f"{self.agent_url}/api/send-message",
                    json={
                        "phone_number": phone_number,
                        "message": message,
                    }
                )
                if response.status_code == 200:
                    logger.info(f"Message sent to {phone_number}")
                    return True
                else:
                    logger.error(f"Failed to send message to {phone_number}: {response.status_code} {response.text}")
                    return False
        except Exception as e:
            logger.error(f"Error sending message to {phone_number}: {e}")
            return False

    def send_report_sync(self, phone_number: str, report_text: str,
                         client_name: str = "") -> bool:
        """Send a tracking report via WhatsApp (synchronous)."""
        try:
            with httpx.Client(timeout=30.0, headers={"X-API-Key": self.api_key}) as client:
                response = client.post(
                    f"{self.agent_url}/api/send-report",
                    json={
                        "phone_number": phone_number,
                        "report": report_text,
                        "client_name": client_name,
                    }
                )
                if response.status_code == 200:
                    logger.info(f"Report sent to {phone_number} for {client_name}")
                    return True
                else:
                    logger.error(f"Failed to send report to {phone_number}: {response.status_code}")
                    return False
        except Exception as e:
            logger.error(f"Error sending report to {phone_number}: {e}")
            return False

    def send_alert_sync(self, phone_number: str, alert_message: str) -> bool:
        """Send an alert message to admin via WhatsApp (synchronous)."""
        return self.send_message_sync(phone_number, f"🚨 *ALERTA SonIA Tracker*\n\n{alert_message}")

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()
