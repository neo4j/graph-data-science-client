from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

import neo4j


def parse_gds_datetime(value: str) -> datetime:
    """Parse a GDS ZonedDateTime string into a UTC-normalized Python datetime.

    Handles all known server formats:
    - ``2026-08-27T20:30:55Z`` (old server, no fractional seconds, no zone bracket)
    - ``2026-08-27T20:30:55.0Z[Z]`` (new serializer, ZoneOffset, 0 nanos)
    - ``2026-08-27T20:30:55.123456789Z[UTC]`` (new serializer, 9-digit nanos)
    - ``2025-01-02T12:39:46.745137+01:00[Europe/Berlin]`` (non-UTC zone, normalized to UTC)
    """
    # Strip Java-style zone-id bracket: "...Z[UTC]" -> "...Z"
    value = re.sub(r"\[.*\]$", "", value)
    # Trim nanosecond precision (9+ digits) to microsecond (6 digits)
    value = re.sub(r"\.(\d{6})\d+", r".\1", value)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    # Normalize to UTC so all datetimes have a consistent timezone regardless of server zone
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_gds_datetime(value: Any) -> Any:
    """Pydantic ``field_validator(mode="before")`` helper for GDS datetime fields.

    Handles ``str`` (Arrow path), ``neo4j.time.DateTime`` (Cypher path), and passthrough.
    """
    match value:
        case str():
            return parse_gds_datetime(value)
        case neo4j.time.DateTime():
            return value.to_native()
        case _:
            return value
