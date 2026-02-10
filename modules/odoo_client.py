"""
SonIA Core — Odoo Client Module
Queries Odoo for client contacts to send WhatsApp reports.
Uses tenant number mapping from custom Odoo field.
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
                logger.error("Odoo authentication failed - no UID returned")
                return False
        except Exception as e:
            logger.error(f"Odoo authentication error: {e}")
            return False

    def _execute(self, model: str, method: str, *args, **kwargs):
        """Execute an Odoo XML-RPC call."""
        if not self.uid or not self._models:
            raise RuntimeError("Not authenticated. Call authenticate() first.")
        return self._models.execute_kw(
            self.db, self.uid, self.password,
            model, method, *args, **kwargs
        )
    def search_companies(self, domain=None) -> List[Dict]:
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

    def find_company_by_tenant_number(self, tenant_number: int,
                                       field_name: str = "x_studio_tenant") -> Optional[Dict]:
        """
        Find a company by its tenant number (custom Odoo field).

        Danny maintains a 'Tenant #' field in Odoo that maps to DynamoDB tenant IDs.
        The field technical name defaults to 'x_studio_tenant' (Odoo Studio convention).
        """
        try:
            companies = self._execute(
                "res.partner", "search_read",
                [[[field_name, "=", tenant_number], ["is_company", "=", True]]],
                {"fields": ["id", "name", "phone", "mobile", "email", "city", "country_id"],
                 "limit": 1}
            )
            if companies:
                logger.info(f"Found company '{companies[0]['name']}' for tenant #{tenant_number}")
                return companies[0]
            else:
                logger.warning(f"No company found in Odoo for tenant #{tenant_number}")
                return None
        except Exception as e:
            logger.error(f"Error finding company by tenant #{tenant_number}: {e}")
            return None
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
                whatsapp = self._clean_phone(c.get("mobile")) or self._clean_phone(c.get("phone"))
                result.append({
                    "id": c["id"],
                    "name": c.get("name", ""),
                    "email": c.get("email", ""),
                    "function": c.get("function", ""),
                    "whatsapp_number": whatsapp,
                })

            logger.info(f"Found {len(result)} contacts for company ID {company_id}")
            return result
        except Exception as e:
            logger.error(f"Error getting contacts for company {company_id}: {e}")
            return []

    def get_whatsapp_contacts_for_company(self, company_id: int) -> List[Dict]:
        """Get only contacts that have WhatsApp numbers."""
        contacts = self.get_contacts_for_company(company_id)
        wa_contacts = [c for c in contacts if c.get("whatsapp_number")]
        logger.info(f"Found {len(wa_contacts)} WhatsApp contacts for company ID {company_id}")
        return wa_contacts
    def find_company_by_name(self, name: str) -> Optional[Dict]:
        """Find a company by name (partial match)."""
        try:
            companies = self._execute(
                "res.partner", "search_read",
                [[["name", "ilike", name], ["is_company", "=", True]]],
                {"fields": ["id", "name", "phone", "mobile", "email"],
                 "limit": 1}
            )
            return companies[0] if companies else None
        except Exception as e:
            logger.error(f"Error finding company by name '{name}': {e}")
            return None

    @staticmethod
    def _clean_phone(phone: str) -> Optional[str]:
        """Clean a phone number for WhatsApp (digits only, with country code)."""
        if not phone:
            return None
        cleaned = "".join(c for c in phone if c.isdigit())
        if len(cleaned) >= 10:
            return cleaned
        return None
