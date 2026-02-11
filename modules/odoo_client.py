"""
SonIA Core — Odoo Client Module
Queries Odoo for client contacts and spreadsheet data.
Reads WhatsApp BBDD spreadsheet for tenant mapping and contacts.
"""

import logging
import json
import base64
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

    # ================================================================
    # SPREADSHEET METHODS
    # ================================================================

    def test_spreadsheet_access(self, doc_id: int) -> Dict:
        """Diagnostic: try multiple approaches to read spreadsheet."""
        results = {}
        try:
            data = self._execute("documents.document", "read", [[doc_id]], {"fields": ["name", "handler", "spreadsheet_data"]})
            if data:
                rec = data[0]
                sd = rec.get("spreadsheet_data", "")
                results["a1"] = {"status": "ok", "name": rec.get("name"), "handler": rec.get("handler"), "data_len": len(sd) if sd else 0, "preview": sd[:300] if sd else None}
            else:
                results["a1"] = {"status": "empty"}
        except Exception as e:
            results["a1"] = {"status": "error", "error": str(e)}
        try:
            fields = self._execute("documents.document", "fields_get", [], {"attributes": ["string", "type"]})
            relevant = {k: v for k, v in fields.items() if "spread" in k.lower() or "data" in k.lower() or "raw" in k.lower()}
            results["fields"] = {"status": "ok", "total": len(fields), "relevant": relevant}
        except Exception as e:
            results["fields"] = {"status": "error", "error": str(e)}
        try:
            data = self._execute("documents.document", "join_spreadsheet_session", [[doc_id]])
            if data:
                results["a4"] = {"status": "ok", "keys": list(data.keys()) if isinstance(data, dict) else type(data).__name__, "preview": str(data)[:300]}
        except Exception as e:
            results["a4"] = {"status": "error", "error": str(e)}
        return results

    def read_spreadsheet(self, doc_id: int) -> Optional[Dict]:
        """Read Odoo Documents spreadsheet, return parsed JSON."""
        try:
            data = self._execute("documents.document", "read", [[doc_id]], {"fields": ["name", "spreadsheet_data"]})
            if data and data[0].get("spreadsheet_data"):
                raw = data[0]["spreadsheet_data"]
                return json.loads(raw) if isinstance(raw, str) else raw
        except Exception as e:
            logger.warning(f"spreadsheet_data failed: {e}")
        try:
            data = self._execute("documents.document", "join_spreadsheet_session", [[doc_id]])
            if data and isinstance(data, dict):
                raw = data.get("data", data.get("spreadsheet_data", ""))
                if isinstance(raw, str) and raw:
                    return json.loads(raw)
                return data
        except Exception as e:
            logger.warning(f"join_spreadsheet_session failed: {e}")
        try:
            data = self._execute("documents.document", "read", [[doc_id]], {"fields": ["datas"]})
            if data and data[0].get("datas"):
                return json.loads(base64.b64decode(data[0]["datas"]).decode("utf-8"))
        except Exception as e:
            logger.warning(f"datas binary failed: {e}")
        logger.error(f"All approaches to read spreadsheet {doc_id} failed")
        return None

    def get_whatsapp_bbdd(self, doc_id: int) -> Dict:
        """Read WhatsApp BBDD spreadsheet and parse both sheets."""
        spreadsheet = self.read_spreadsheet(doc_id)
        if not spreadsheet:
            return {"tenant_mapping": {}, "contacts": []}
        result = {"tenant_mapping": {}, "contacts": []}
        sheets = spreadsheet.get("sheets", [])
        for sheet in sheets:
            name = sheet.get("name", "").lower().strip()
            cells = sheet.get("cells", {})
            if "hoja 2" in name or "hoja2" in name or name == "sheet2":
                result["tenant_mapping"] = self._parse_tenant_mapping(cells)
            elif "hoja 1" in name or "hoja1" in name or name == "sheet1":
                result["contacts"] = self._parse_contacts(cells)
        logger.info(f"WhatsApp BBDD: {len(result['tenant_mapping'])} tenants, {len(result['contacts'])} contacts")
        return result

    def _parse_tenant_mapping(self, cells: Dict) -> Dict[int, str]:
        """Parse Hoja 2 cells: tenant_number -> client_name."""
        mapping = {}
        row = 2
        while True:
            a = cells.get(f"A{row}", {})
            b = cells.get(f"B{row}", {})
            tv = a.get("content", a.get("value", ""))
            cv = b.get("content", b.get("value", ""))
            if not tv and not cv:
                break
            try:
                mapping[int(tv)] = str(cv).strip()
            except (ValueError, TypeError):
                pass
            row += 1
        return mapping

    def _parse_contacts(self, cells: Dict) -> List[Dict]:
        """Parse Hoja 1 cells into contact list."""
        contacts = []
        row = 2
        while True:
            a = cells.get(f"A{row}", {})
            h = cells.get(f"H{row}", {})
            cliente = a.get("content", a.get("value", ""))
            ts = h.get("content", h.get("value", ""))
            if not cliente and not ts:
                break
            if ts:
                try:
                    tn = int(ts)
                except (ValueError, TypeError):
                    tn = None
                if tn is not None:
                    b = cells.get(f"B{row}", {})
                    c = cells.get(f"C{row}", {})
                    d = cells.get(f"D{row}", {})
                    e = cells.get(f"E{row}", {})
                    contacts.append({"cliente": str(cliente).strip(), "nombre": str(b.get("content", b.get("value", ""))).strip(), "nickname": str(c.get("content", c.get("value", ""))).strip(), "whatsapp": str(d.get("content", d.get("value", ""))).strip(), "rol": str(e.get("content", e.get("value", ""))).strip(), "tenant_number": tn})
            row += 1
        return contacts

    # ================================================================
    # LEGACY METHODS
    # ================================================================

    def search_companies(self, domain=None) -> List[Dict]:
        """Search for companies in Odoo."""
        try:
            if domain is None:
                domain = [["is_company", "=", True], ["customer_rank", ">", 0]]
            return self._execute("res.partner", "search_read", [domain], {"fields": ["id", "name", "phone", "mobile", "email", "city", "country_id"]})
        except Exception as e:
            logger.error(f"Error searching companies: {e}")
            return []

    def find_company_by_tenant_number(self, tenant_number: int, field_name: str = "x_studio_tenant") -> Optional[Dict]:
        """Find company by tenant number."""
        try:
            r = self._execute("res.partner", "search_read", [[[field_name, "=", tenant_number], ["is_company", "=", True]]], {"fields": ["id", "name", "phone", "mobile", "email", "city", "country_id"], "limit": 1})
            return r[0] if r else None
        except Exception as e:
            logger.error(f"Error finding company by tenant #{tenant_number}: {e}")
            return None

    def find_company_by_name(self, name: str) -> Optional[Dict]:
        """Find company by name (partial match)."""
        try:
            r = self._execute("res.partner", "search_read", [[["name", "ilike", name], ["is_company", "=", True]]], {"fields": ["id", "name", "phone", "mobile", "email"], "limit": 1})
            return r[0] if r else None
        except Exception as e:
            logger.error(f"Error finding company '{name}': {e}")
            return None

    @staticmethod
    def _clean_phone(phone: str) -> Optional[str]:
        """Clean phone for WhatsApp (digits only, with country code)."""
        if not phone:
            return None
        cleaned = "".join(c for c in phone if c.isdigit())
        return cleaned if len(cleaned) >= 10 else None
