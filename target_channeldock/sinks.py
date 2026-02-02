"""Sink implementations for ChannelDock."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from target_channeldock.client import ChannelDockBaseSink


class BuyOrdersSink(ChannelDockBaseSink):
    """ChannelDock sink for BuyOrders stream (Deliveries)."""

    endpoint = "/seller/delivery"
    name = "BuyOrders"

    def _parse_line_items(self, line_items_field: Any) -> list:
        """Parse line_items from JSON string or return as list."""
        if not line_items_field:
            return []
        if isinstance(line_items_field, str):
            try:
                return json.loads(line_items_field)
            except json.JSONDecodeError:
                self.logger.warning(f"Failed to parse line_items as JSON: {line_items_field[:100] if len(str(line_items_field)) > 100 else line_items_field}")
                return []
        if isinstance(line_items_field, list):
            return line_items_field
        return []

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Transform BuyOrder record to ChannelDock delivery format."""
        
        # Check if this is the original record (has line_items) or mapped record
        has_line_items = "line_items" in record or "lineItems" in record
        
        # Only transform if we have the original record
        if not has_line_items:
            # This is the mapped record, skip transformation
            self.logger.info("Skipping transformation for mapped record")
            return record
        
        payload = {}
        line_items = self._parse_line_items(record.get("line_items"))

        # Map ref from externalid
        ref = record.get("externalid")
        if ref is not None:
            payload["ref"] = str(ref)

        # Map supplier_id from supplier_remoteId
        supplier_id = record.get("supplier_remoteId")
        if supplier_id is not None:
            payload["supplier_id"] = supplier_id

        # Map delivery_date from transaction_date
        transaction_date = record.get("transaction_date")
        if transaction_date is not None:
            payload["delivery_date"] = self._format_date(transaction_date, "YYYY-MM-DD")

        # Set default delivery_type
        payload["delivery_type"] = "inbound"

        # Process line items
        items = []
        for line in line_items:
            item = {}
            
            # Map ean from optiplyWebshopProductRemoteID
            ean = line.get("eanCode")
            if ean is not None:
                item["ean"] = str(ean)
            
            # Map amount from quantity
            quantity = line.get("quantity")
            if quantity is not None:
                item["amount"] = quantity
            
            if item:
                items.append(item)
        
        if items:
            payload["items"] = items

        self.logger.info(f"Final payload: {json.dumps(payload)}")
        
        # Store payload for upsert_record
        self._current_payload = payload
        
        return record  # Return original record, SDK will use stored payload

    def _format_date(self, value, date_format: str) -> str:
        """Format date string/datetime to target format."""
        if not value:
            return ""

        try:
            # Handle datetime objects
            if isinstance(value, datetime):
                return value.strftime("%Y-%m-%d")

            # Handle string values
            if isinstance(value, str):
                # Try parsing common date formats
                for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"]:
                    try:
                        dt = datetime.strptime(value, fmt)
                        return dt.strftime("%Y-%m-%d")
                    except ValueError:
                        continue

            # If no format matches, return original value
            return str(value)
        except Exception as e:
            self.logger.warning(f"Failed to format date '{value}': {e}")
            return str(value)

    def upsert_record(self, record: dict, context: dict) -> tuple[str, bool, dict]:
        """Send the delivery record to ChannelDock API."""
        state_updates = {}

        try:
            # Use stored payload if available, otherwise transform record
            payload = getattr(self, "_current_payload", None)
            if payload is None:
                # Fallback: transform the record
                payload = self.preprocess_record(record, context)
            
            # Clear stored payload
            self._current_payload = None

            self.logger.info(f"Sending payload to {self.endpoint}: {json.dumps(payload)}")

            # Make API request
            response = self.request_api(
                "POST",
                self.endpoint,
                request_data=payload,
            )

            # Extract delivery ID from response
            response_data = response.json()
            delivery_id = response_data.get("delivery_id") or response_data.get(
                "id"
            ) or record.get("externalid", "")

            return delivery_id, True, state_updates

        except Exception as e:
            import traceback
            self.logger.error(f"Failed to create delivery: {e}")
            self.logger.error(traceback.format_exc())
            return "", False, state_updates
