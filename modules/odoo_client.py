"""
SonIA Core â Odoo Client Module
Queries Odoo for client contacts to send WhatsApp reports.
"""

import logging
import xmlrpc.client
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class OdooClient:
    """Client for Odoo XML-RPC API."""

    def __init__(self, url: str, db: str, username: str, password: str):
        self.url = url.rstrip("/")
        self.db = db
        self.username = username
        self.password = password
        self.uid = None
        self._common = None
        self._models = None

    def authenticate(self) -> bool:
        """Authenticate with Odoo and get UID."""
        try:
            self._common = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/common")
            self.uid = self._common.authenticate(self.db, self.username, self.password, {})
            if self.uid:
                self._models = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/object")
                logger.info(f"Odoo authentication successful (UID: {self.uid})")
                return True
            else:
                logger.error("Odoo authentication failed: invalid credentials")
                return False
        except Exception as e:
            logger.error(f"Odoo authentication error: {e}")
            return False

    def _execute(self, model: str, method: str, *args, **kwargs):
        """Execute an Odoo RPC call."""
        if not self.uid or not self._models:
            raise RuntimeError("Not authenticated with Odoo")
        return self._models.execute_kw(
            self.db, self.uid, self.password, model, method, *args, **kwargs
        )

    def search_companies(self, domain: List = None) -> List[Dict]:
        """Search for companies in Odoo."""
        try:
            if domain is None:
                domain = [["is_company", "=", True], ["customer_rank", ">", 0]]

            companies = self._execute(
                "res.partner", "search_read",
                [domain],
                {"fields": ["id", "name", "phone", "mobile", "email", "city", "country_id"]}
            )
            logger.info(f"Found {len(companies)} companies in Odoo")
            return companies
        except Exception as e:
            logger.error(f"Error searching companies: {e}")
            return []

    def get_contacts_for_company(self, company_id: int) -> List[Dict]:
        """Get all contacts (people) belonging to a company."""
        try:
            contacts = self._execute(
                "res.partner", "search_read",
                [[["parent_id", "=", company_id], ["is_company", "=", False]]],
                {"fields": ["id", "name", "phone", "mobile", "email", "function"]}
            )

            # Build clean contact list with WhatsApp numbers
            result = []
            for c in contacts:
                whatsapp = c.get("mobile") or c.get("phone") or ""
                # Clean WhatsApp number
                whatsapp = whatsapp.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
                if whatsapp and not whatsapp.startswith("+"):
                    if len(whatsapp) == 10:  # US number
                        whatsapp = "1" + whatsapp

                result.append({
                    "odoo_id": c["id"],
                    "name": c.get("name", ""),
                    "whatsapp": whatsapp,
                    "email": c.get("email", ""),
                    "function": c.get("function", ""),
                })

            logger.info(f"Found {len(result)} contacts for company ID {company_id}")
            return result
        except Exception as e:
            logger.error(f"Error getting contacts for company {company_id}: {e}")
            return []

    def find_company_by_name(self, name: str) -> Optional[Dict]:
        """Find a company by name (fuzzy match)."""
        try:
            # Try exact match first
            companies = self._execute(
                "res.partner", "search_read",
                [[["is_company", "=", True], ["name", "ilike", name]]],
                {"fields": ["id", "name", "phone", "mobile", "email"], "limit": 5}
            )

            if companies:
                # Return the best match (exact or first ilike result)
                for c in companies:
                    if c["name"].lower() == name.lower():
                        return c
                return companies[0]

            return None
        except Exception as e:
            logger.error(f"Error finding company '{name}': {e}")
            return None
