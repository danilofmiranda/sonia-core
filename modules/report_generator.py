"""
SonIA Core — Report Generator Module
Generates per-client tracking reports for WhatsApp delivery.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

COT = timezone(timedelta(hours=-5))


class ReportGenerator:
    """Generates tracking reports for each client."""

    @staticmethod
    def generate_client_report(client_name: str, shipments: List[Dict]) -> str:
        """
        Generate a WhatsApp-friendly text report for a client.
        Includes all their shipments (delivered and in-transit).
        """
        now = datetime.now(COT)
        date_str = now.strftime("%d/%m/%Y")
        time_str = now.strftime("%I:%M %p")

        # Separate delivered vs active
        delivered = [s for s in shipments if s.get("is_delivered")]
        active = [s for s in shipments if not s.get("is_delivered")]

        # Build report
        lines = []
        lines.append(f"📦 *Reporte de Envíos — {client_name}*")
        lines.append(f"📅 {date_str} | ⏰ {time_str} COT")
        lines.append(f"━━━━━━━━━━━━━━━━━━━━━")
        lines.append(f"Total guías: {len(shipments)} | ✅ Entregadas: {len(delivered)} | 🚚 Activas: {len(active)}")
        lines.append("")

        # Active shipments first (most important)
        if active:
            lines.append("🚚 *ENVÍOS ACTIVOS:*")
            lines.append("")
            for s in active:
                status_emoji = ReportGenerator._status_emoji(s.get("sonia_status", "unknown"))
                lines.append(f"{status_emoji} *{s.get('tracking_number', 'N/A')}*")
                lines.append(f"   Estado: {ReportGenerator._status_display(s.get('sonia_status', 'unknown'))}")
                if s.get("fedex_status"):
                    lines.append(f"   FedEx: {s['fedex_status']}")
                if s.get("ship_date"):
                    lines.append(f"   Enviado: {s['ship_date']}")
                if s.get("destination_city"):
                    dest = s["destination_city"]
                    if s.get("destination_state"):
                        dest += f", {s['destination_state']}"
                    lines.append(f"   Destino: {dest}")
                lines.append("")

        # Delivered summary (compact)
        if delivered:
            lines.append(f"✅ *ENTREGADAS ({len(delivered)}):*")
            lines.append("")
            for s in delivered[:10]:  # Show first 10
                lines.append(f"   ✅ {s.get('tracking_number', 'N/A')} — {s.get('delivery_date', 'N/A')}")
            if len(delivered) > 10:
                lines.append(f"   ... y {len(delivered) - 10} más")
            lines.append("")

        lines.append("━━━━━━━━━━━━━━━━━━━━━")
        lines.append("🤖 _Generado por SonIA Tracker — BloomsPal_")

        return "\n".join(lines)

    @staticmethod
    def generate_admin_inconsistency_alert(client_name: str, tenant_id: int,
                                            pending_count: int) -> str:
        """Generate an alert message for admin when client not found in Odoo."""
        return (
            f"⚠️ *Inconsistencia detectada*\n\n"
            f"No se encontró en Odoo al cliente con tenant ID *{tenant_id}*"
            f"{f' ({client_name})' if client_name else ''}.\n\n"
            f"📦 Guías pendientes: *{pending_count}*\n\n"
            f"Por favor verifica el mapeo de tenants en la tabla de configuración."
        )

    @staticmethod
    def generate_anomaly_alert(tracking_number: str, client_name: str,
                               rule: str, details: str = "") -> str:
        """Generate an alert for an anomaly detection."""
        rule_descriptions = {
            "exception_detected": "🔴 Excepción de entrega",
            "transit_too_long": "🟠 Demasiado tiempo en tránsito",
            "returned_to_sender": "🔴 Devuelto a origen",
            "delivery_attempted_stuck": "🟡 Intento de entrega sin éxito",
            "customs_too_long": "🟠 Demasiado tiempo en aduanas",
            "label_no_movement": "🟡 Label sin movimiento",
        }

        rule_desc = rule_descriptions.get(rule, rule)

        msg = (
            f"🚨 *Anomalía detectada*\n\n"
            f"Guía: *{tracking_number}*\n"
            f"Cliente: {client_name}\n"
            f"Problema: {rule_desc}\n"
        )
        if details:
            msg += f"Detalle: {details}\n"
        msg += f"\n📋 Se creó reclamo automático en el Portal."

        return msg

    @staticmethod
    def _status_emoji(status: str) -> str:
        emojis = {
            "label_created": "🏷️",
            "picked_up": "📥",
            "in_transit": "🚚",
            "in_customs": "🛃",
            "out_for_delivery": "🏃",
            "delivered": "✅",
            "exception": "🔴",
            "delayed": "🟠",
            "on_hold": "⏸️",
            "delivery_attempted": "🟡",
            "returned_to_sender": "↩️",
            "cancelled": "❌",
            "unknown": "❓",
        }
        return emojis.get(status, "❓")

    @staticmethod
    def _status_display(status: str) -> str:
        display = {
            "label_created": "Label Creada",
            "picked_up": "Recogido",
            "in_transit": "En Tránsito",
            "in_customs": "En Aduanas",
            "out_for_delivery": "En camino para entrega",
            "delivered": "Entregado",
            "exception": "Excepción",
            "delayed": "Retrasado",
            "on_hold": "En espera",
            "delivery_attempted": "Intento de entrega",
            "returned_to_sender": "Devuelto a origen",
            "cancelled": "Cancelado",
            "unknown": "Desconocido",
        }
        return display.get(status, status)
