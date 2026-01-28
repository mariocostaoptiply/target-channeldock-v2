"""ChannelDock target class."""

from __future__ import annotations

from singer_sdk import typing as th
from target_hotglue.target import TargetHotglue

from target_channeldock.sinks import BuyOrdersSink


class TargetChannelDock(TargetHotglue):
    """Target for ChannelDock using Hotglue Target SDK."""

    name = "target-channeldock"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            description="ChannelDock API Key",
        ),
        th.Property(
            "api_secret",
            th.StringType,
            required=True,
            description="ChannelDock API Secret",
        ),
        th.Property(
            "url_base",
            th.StringType,
            default="https://channeldock.com/portal/api/v2",
            description="ChannelDock API base URL",
        ),
    ).to_dict()

    SINK_TYPES = [BuyOrdersSink]


if __name__ == "__main__":
    TargetChannelDock.cli()
