"""
SonIA Core - Odoo Client Module
Connects to Odoo via XML-RPC to read WhatsApp BBDD spreadsheet.
"""

import json
import base64
import logging
import zlib
from typing import Dict, List, Optional, Any
from xmlrpc import client as xmlrpc_client

logger = logging.getLogger(__name__)


class OdooClient:
    def __init__(self, url: str, db: str, username: str, password: str):
        self.url = url
        self.db = db
        self.username = username
        self.password = password
        self.uid = None
        self.common = None
        self.models = None

    def authenticate(self) -> bool:
        try:
            self.common = xmlrpc_client.ServerProxy(f"{self.url}/xmlrpc/2/common")
            self.uid = self.common.authenticate(self.db, self.username, self.password, {})
            if self.uid:
                self.models = xmlrpc_client.ServerProxy(f"{self.url}/xmlrpc/2/object")
                logger.info(f"Odoo auth OK - uid={self.uid}")
                return True
            logger.error("Odoo auth failed - no uid")
            return False
        except Exception as e:
            logger.error(f"Odoo auth error: {e}")
            return False

    def _execute(self, model: str, method: str, *args, **kwargs):
        return self.models.execute_kw(
            self.db, self.uid, self.password, model, method, *args, **kwargs
        )

    # ==================================================================
    # SPREADSHEET ACCESS - Multiple approaches for Odoo Documents
    # ==================================================================

    def test_spreadsheet_access(self, doc_id: int) -> Dict:
        """Diagnostic: try multiple approaches to read spreadsheet data."""
        results = {}

        # Approach a1: spreadsheet_data field (template/structure only)
        try:
            records = self._execute(
                "documents.document", "read",
                [[doc_id]],
                {"fields": ["name", "handler", "spreadsheet_data"]}
            )
            if records:
                r = records[0]
                sd = r.get("spreadsheet_data", "")
                results["a1_spreadsheet_data"] = {
                    "status": "ok",
                    "name": r.get("name"),
                    "handler": r.get("handler"),
                    "data_len": len(sd) if sd else 0,
                    "preview": sd[:200] if sd else None,
                }
        except Exception as e:
            results["a1_spreadsheet_data"] = {"status": "error", "error": str(e)}

        # Approach a2: spreadsheet_snapshot (full state with cell data)
        try:
            records = self._execute(
                "documents.document", "read",
                [[doc_id]],
                {"fields": ["spreadsheet_snapshot"]}
            )
            if records:
                snapshot = records[0].get("spreadsheet_snapshot")
                if snapshot:
                    # snapshot is base64 encoded, possibly gzipped
                    raw = base64.b64decode(snapshot)
                    try:
                        decompressed = zlib.decompress(raw, 15 + 32)
                        text = decompressed.decode("utf-8")
                    except Exception:
                        text = raw.decode("utf-8", errors="replace")
                    parsed = json.loads(text)
                    sheet_names = [s.get("name") for s in parsed.get("sheets", [])]
                    cell_counts = {}
                    for s in parsed.get("sheets", []):
                        cell_counts[s.get("name", "?")] = len(s.get("cells", {}))
                    results["a2_snapshot"] = {
                        "status": "ok",
                        "data_len": len(text),
                        "sheets": sheet_names,
                        "cell_counts": cell_counts,
                        "preview": text[:200],
                    }
                else:
                    results["a2_snapshot"] = {"status": "empty", "note": "snapshot field is empty"}
        except Exception as e:
            results["a2_snapshot"] = {"status": "error", "error": str(e)[:300]}

        # Approach a3: spreadsheet_binary_data
        try:
            records = self._execute(
                "documents.document", "read",
                [[doc_id]],
                {"fields": ["spreadsheet_binary_data"]}
            )
            if records:
                bin_data = records[0].get("spreadsheet_binary_data")
                if bin_data:
                    raw = base64.b64decode(bin_data)
                    try:
                        decompressed = zlib.decompress(raw, 15 + 32)
                        text = decompressed.decode("utf-8")
                    except Exception:
                        text = raw.decode("utf-8", errors="replace")
                    try:
                        parsed = json.loads(text)
                        sheet_names = [s.get("name") for s in parsed.get("sheets", [])]
                        cell_counts = {}
                        for s in parsed.get("sheets", []):
                            cell_counts[s.get("name", "?")] = len(s.get("cells", {}))
                        results["a3_binary_data"] = {
                            "status": "ok",
                            "data_len": len(text),
                            "sheets": sheet_names,
                            "cell_counts": cell_counts,
                        }
                    except json.JSONDecodeError:
                        results["a3_binary_data"] = {
                            "status": "ok_raw",
                            "data_len": len(text),
                            "preview": text[:200],
                        }
                else:
                    results["a3_binary_data"] = {"status": "empty"}
        except Exception as e:
            results["a3_binary_data"] = {"status": "error", "error": str(e)[:300]}

        # Approach a4: fields_get to list all relevant fields
        try:
            all_fields = self._execute(
                "documents.document", "fields_get",
                [],
                {"attributes": ["string", "type"]}
            )
            relevant = {k: v for k, v in all_fields.items()
                        if "spread" in k.lower() or "data" in k.lower()
                        or k in ("datas", "raw")}
            results["fields"] = {
                "status": "ok",
                "total": len(all_fields),
                "relevant": relevant,
            }
        except Exception as e:
            results["fields"] = {"status": "error", "error": str(e)[:200]}

        return results

    def read_spreadsheet(self, doc_id: int) -> Optional[Dict]:
        """Read Odoo Documents spreadsheet, return parsed JSON with cell data."""

        # Try 1: spreadsheet_snapshot (most likely to have full data)
        try:
            records = self._execute(
                "documents.document", "read",
                [[doc_id]],
                {"fields": ["spreadsheet_snapshot"]}
            )
            if records:
                snapshot = records[0].get("spreadsheet_snapshot")
                if snapshot:
                    raw = base64.b64decode(snapshot)
                    try:
                        text = zlib.decompress(raw, 15 + 32).decode("utf-8")
                    except Exception:
                        text = raw.decode("utf-8", errors="replace")
                    parsed = json.loads(text)
                    sheets = parsed.get("sheets", [])
                    if sheets and any(len(s.get("cells", {})) > 0 for s in sheets):
                        logger.info(f"Read spreadsheet {doc_id} via snapshot: {len(sheets)} sheets")
                        return parsed
                    logger.info("Snapshot exists but has no cell data, trying next approach")
        except Exception as e:
            logger.warning(f"Snapshot approach failed: {e}")

        # Try 2: spreadsheet_binary_data
        try:
            records = self._execute(
                "documents.document", "read",
                [[doc_id]],
                {"fields": ["spreadsheet_binary_data"]}
            )
            if records:
                bin_data = records[0].get("spreadsheet_binary_data")
                if bin_data:
                    raw = base64.b64decode(bin_data)
                    try:
                        text = zlib.decompress(raw, 15 + 32).decode("utf-8")
                    except Exception:
                        text = raw.decode("utf-8", errors="replace")
                    parsed = json.loads(text)
                    sheets = parsed.get("sheets", [])
                    if sheets and any(len(s.get("cells", {})) > 0 for s in sheets):
                        logger.info(f"Read spreadsheet {doc_id} via binary_data: {len(sheets)} sheets")
                        return parsed
        except Exception as e:
            logger.warning(f"Binary data approach failed: {e}")

        # Try 3: spreadsheet_data (base template - may lack cell data)
        try:
            records = self._execute(
                "documents.document", "read",
                [[doc_id]],
                {"fields": ["spreadsheet_data"]}
            )
            if records:
                sd = records[0].get("spreadsheet_data")
                if sd:
                    parsed = json.loads(sd)
                    logger.info(f"Read spreadsheet {doc_id} via spreadsheet_data")
                    return parsed
        except Exception as e:
            logger.warning(f"spreadsheet_data approach failed: {e}")

        logger.error(f"All approaches failed for spreadsheet {doc_id}")
        return None

    def get_whatsapp_bbdd(self, doc_id: int) -> Dict:
        """Read WhatsApp BBDD spreadsheet and parse both sheets."""
        data = self.read_spreadsheet(doc_id)
        if not data:
            return {"tenant_mapping": {}, "contacts": [], "error": "Could not read spreadsheet"}

        sheets = data.get("sheets", [])
        tenant_mapping = {}
        contacts = []

        for sheet in sheets:
            name = sheet.get("name", "").strip().lower()
            cells = sheet.get("cells", {})

            if "hoja2" in name or "hoja 2" in name:
                tenant_mapping = self._parse_tenant_mapping(cells)
            elif "hoja1" in name or "hoja 1" in name:
                contacts = self._parse_contacts(cells)

        return {
            "tenant_mapping": tenant_mapping,
            "contacts": contacts,
            "sheets_found": [s.get("name") for s in sheets],
        }

    def _parse_tenant_mapping(self, cells: Dict) -> Dict[int, str]:
        """Parse Hoja 2 cells: tenant_number (col A) -> client_name (col B)."""
        mapping = {}
        row = 2  # Skip header row
        while row < 100:
            tenant_cell = cells.get(f"A{row}", {})
            client_cell = cells.get(f"B{row}", {})
            tenant_val = tenant_cell.get("content", "")
            client_val = client_cell.get("content", "")

            if not tenant_val and not client_val:
                break

            try:
                tenant_num = int(str(tenant_val).strip())
                client_name = str(client_val).strip()
                if client_name:
                    mapping[tenant_num] = client_name
            except (ValueError, TypeError):
                pass
            row += 1

        logger.info(f"Parsed {len(mapping)} tenant mappings from Hoja 2")
        return mapping

    def _parse_contacts(self, cells: Dict) -> List[Dict]:
        """Parse Hoja 1 cells into contact list."""
        contacts = []
        row = 2  # Skip header
        while row < 200:
            cliente = cells.get(f"A{row}", {}).get("content", "")
            nombre = cells.get(f"B{row}", {}).get("content", "")
            nickname = cells.get(f"C{row}", {}).get("content", "")
            whatsapp = cells.get(f"D{row}", {}).get("content", "")
            rol = cells.get(f"E{row}", {}).get("content", "")
            clave = cells.get(f"F{row}", {}).get("content", "")
            bloqueo = cells.get(f"G{row}", {}).get("content", "")
            tenant_num = cells.get(f"H{row}", {}).get("content", "")

            if not cliente and not nombre and not whatsapp:
                break

            contact = {
                "cliente": str(cliente).strip(),
                "nombre_usuario": str(nombre).strip(),
                "nickname": str(nickname).strip(),
                "whatsapp": self._clean_phone(str(whatsapp).strip()),
                "rol": str(rol).strip(),
                "clave": str(clave).strip(),
                "bloqueo": str(bloqueo).strip(),
                "tenant_number": None,
            }

            if tenant_num:
                try:
                    contact["tenant_number"] = int(str(tenant_num).strip())
                except (ValueError, TypeError):
                    pass

            contacts.append(contact)
            row += 1

        logger.info(f"Parsed {len(contacts)} contacts from Hoja 1")
        return contacts

    # ==================================================================
    # LEGACY METHODS (kept for backwards compatibility)
    # ==================================================================

    def search_companies(self, limit: int = 50) -> List[Dict]:
        return self._execute(
            "res.partner", "search_read",
            [[["is_company", "=", True]]],
            {"fields": ["id", "name", "email", "phone"], "limit": limit}
        )

    def find_company_by_tenant_number(self, tenant_number: int, field_name: str = "x_studio_tenant") -> Optional[Dict]:
        try:
            results = self._execute(
                "res.partner", "search_read",
                [[[field_name, "=", tenant_number], ["is_company", "=", True]]],
                {"fields": ["id", "name", "email", "phone", field_name], "limit": 1}
            )
            return results[0] if results else None
        except Exception as e:
            logger.error(f"Tenant lookup error: {e}")
            return None

    def find_company_by_name(self, name: str) -> Optional[Dict]:
        try:
            results = self._execute(
                "res.partner", "search_read",
                [[["name", "ilike", name], ["is_company", "=", True]]],
                {"fields": ["id", "name", "email", "phone"], "limit": 5}
            )
            return results[0] if results else None
        except Exception as e:
            logger.error(f"Company name lookup error: {e}")
            return None

    @staticmethod
    def _clean_phone(phone: str) -> str:
        if not phone:
            return ""
        cleaned = "".join(c for c in phone if c.isdigit() or c == "+")
        if cleaned.startswith("+"):
            cleaned = cleaned[1:]
        return cleaned
