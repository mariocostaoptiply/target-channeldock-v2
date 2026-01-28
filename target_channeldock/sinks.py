"""Sink implementations for ChannelDock."""

from __future__ import annotations

import json
from datetime import datetime

from target_channeldock.client import ChannelDockBaseSink


class BuyOrdersSink(ChannelDockBaseSink):
    """ChannelDock sink for BuyOrders stream (Deliveries)."""

    endpoint = "/seller/delivery"
    name = "BuyOrders"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Transform Optiply BuyOrder record to ChannelDock delivery format."""
        
        # Check if this is the original record (has line_items) or mapped record
        has_line_items = "line_items" in record or "lineItems" in record
        
        # Only transform if we have the original record
        if not has_line_items:
            # This is the mapped record, skip transformation
            self.logger.info("Skipping transformation for mapped record")
            return record
        
        payload = {}

        # Load mapping config from .secrets folder
        mapping_config = self._load_mapping_config()
        buy_orders_config = mapping_config.get("buyOrders", {})
        buy_order_lines_config = mapping_config.get("buyOrderLines", {})
        defaults = buy_orders_config.get("defaults", [])

        # Debug: log record keys
        self.logger.info(f"Processing record with keys: {list(record.keys())}")

        # Process field mappings
        for mapping in buy_orders_config.get("mappings", []):
            optiply_field = mapping["optiplyField"]
            target_field = mapping["targetField"]
            transformation = mapping.get("transformation", "direct")
            store_separately = mapping.get("storeSeparately", False)
            required = mapping.get("required", False)

            # Skip fields marked for separate storage (optiplyBuyOrderId for tracking)
            if store_separately:
                self.logger.info(
                    f"Skipping '{optiply_field}' (stored separately for tracking)"
                )
                continue

            # Get value directly from record using the exact field name from data.singer
            value = record.get(optiply_field)

            self.logger.info(
                f"Mapping '{optiply_field}' -> '{target_field}': {value}"
            )

            if value is None:
                if required:
                    self.logger.warning(
                        f"Required field '{optiply_field}' missing from record"
                    )
                continue

            if transformation == "direct":
                payload[target_field] = value

            elif transformation == "date":
                payload[target_field] = self._format_date(value, "YYYY-MM-DD")

            # Add other transformations as needed

        self.logger.info(f"Payload after mappings: {payload}")

        # Apply default values
        for default in defaults:
            target_field = default["targetField"]
            value = default["value"]
            required_default = default.get("required", False)

            if target_field not in payload or (
                required_default and payload.get(target_field) is None
            ):
                payload[target_field] = value

        # Handle nested line items (items[])
        if buy_order_lines_config.get("nestedInOrder", False):
            nested_path = buy_order_lines_config.get("nestedPath", "items[]")
            items = self._process_line_items(
                record, buy_order_lines_config, nested_path
            )
            if items:
                payload["items"] = items

        self.logger.info(f"Final payload: {payload}")
        
        # Store payload for upsert_record
        self._current_payload = payload
        
        return record  # Return original record, SDK will use stored payload

        # Apply default values
        for default in defaults:
            target_field = default["targetField"]
            value = default["value"]
            required_default = default.get("required", False)

            if target_field not in payload or (
                required_default and payload.get(target_field) is None
            ):
                payload[target_field] = value

        # Handle nested line items (items[])
        if buy_order_lines_config.get("nestedInOrder", False):
            nested_path = buy_order_lines_config.get("nestedPath", "items[]")
            items = self._process_line_items(
                record, buy_order_lines_config, nested_path
            )
            if items:
                payload["items"] = items

        self.logger.info(f"Final payload: {payload}")
        return payload

    def _process_line_items(
        self, record: dict, line_config: dict, nested_path: str
    ) -> list:
        """Process BuyOrderLines into ChannelDock items format."""
        items = []

        # Extract the field name from nested path (e.g., "items[]" -> "items")
        items_key = nested_path.replace("[]", "")

        # Get line items from record - handle various field name formats
        line_items = record.get("line_items")
        if line_items is None:
            line_items = record.get("lineItems")
        if line_items is None:
            line_items = record.get("lines")

        # Parse JSON string if needed
        if isinstance(line_items, str):
            try:
                line_items = json.loads(line_items)
            except json.JSONDecodeError:
                self.logger.error(f"Failed to parse line_items as JSON: {line_items}")
                return []

        if not line_items:
            self.logger.warning("No line items found in record")
            return []

        # Process each line item
        for line in line_items:
            item_payload = {}
            for mapping in line_config.get("mappings", []):
                optiply_field = mapping["optiplyField"]
                target_field = mapping["targetField"]

                # Extract field name from nested path (e.g., "items[].ean" -> "ean")
                # Handle both "items[].ean" and "items.ean" formats
                if target_field.startswith(f"{items_key}[]."):
                    field_name = target_field.replace(f"{items_key}[].", "")
                elif target_field.startswith(f"{items_key}."):
                    field_name = target_field.replace(f"{items_key}.", "")
                else:
                    # Fallback: remove any "[]." pattern
                    field_name = target_field.replace("[].", "").replace(f"{items_key}.", "")

                # Get value directly from line item using exact field name from data.singer
                value = line.get(optiply_field)

                if value is not None:
                    item_payload[field_name] = value

            if item_payload:
                items.append(item_payload)

        return items

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

    def _load_mapping_config(self) -> dict:
        """Load mapping configuration from .secrets folder."""
        import os

        config_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            ".secrets",
            "phase1-mapping-config.json",
        )

        if os.path.exists(config_path):
            with open(config_path) as f:
                return json.load(f)

        # Fallback to minimal config if file not found
        # Mappings based on actual field names from data.singer
        return {
            "buyOrders": {
                "endpoint": "/seller/delivery",
                "method": "POST",
                "mappings": [
                    {
                        "optiplyField": "externalid",
                        "targetField": "ref",
                        "transformation": "direct",
                        "required": True,
                    },
                    {
                        "optiplyField": "supplier_remoteId",
                        "targetField": "supplier_id",
                        "transformation": "direct",
                        "required": True,
                    },
                    {
                        "optiplyField": "transaction_date",
                        "targetField": "delivery_date",
                        "transformation": "date",
                        "dateFormat": "YYYY-MM-DD",
                    },
                ],
                "defaults": [
                    {"targetField": "delivery_type", "value": "inbound", "required": True}
                ],
            },
            "buyOrderLines": {
                "nestedInOrder": True,
                "nestedPath": "items[]",
                "mappings": [
                    {
                        "optiplyField": "optiplyWebshopProductRemoteID",
                        "targetField": "items[].ean",
                        "transformation": "direct",
                    },
                    {
                        "optiplyField": "quantity",
                        "targetField": "items[].amount",
                        "transformation": "direct",
                    },
                ],
            },
        }

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

            self.logger.info(f"Sending payload to {self.endpoint}: {payload}")

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
            ) or record.get("optiplyBuyOrderId", "")

            self.logger.info(f"Successfully created delivery: {delivery_id}")

            return delivery_id, True, state_updates

        except Exception as e:
            import traceback
            self.logger.error(f"Failed to create delivery: {e}")
            self.logger.error(traceback.format_exc())
            return "", False, state_updates
