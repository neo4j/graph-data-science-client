from datetime import datetime, timezone

from graphdatascience.utils.gds_datetime import parse_gds_datetime


class TestParseGdsDatetime:
    def test_old_server_format_no_fractional_no_bracket(self) -> None:
        # Old server: "2026-08-27T20:30:55Z" (no fractional seconds, no zone bracket)
        result = parse_gds_datetime("2026-08-27T20:30:55Z")
        assert result == datetime(2026, 8, 27, 20, 30, 55, tzinfo=timezone.utc)

    def test_new_serializer_zero_nanos_zone_offset(self) -> None:
        # New serializer with ZoneOffset.UTC: "2026-08-27T20:30:55.0Z[Z]"
        result = parse_gds_datetime("2026-08-27T20:30:55.0Z[Z]")
        assert result == datetime(2026, 8, 27, 20, 30, 55, 0, tzinfo=timezone.utc)

    def test_new_serializer_zero_nanos_zone_region(self) -> None:
        # New serializer with ZoneId.of("UTC"): "2026-08-27T20:30:55.0Z[UTC]"
        result = parse_gds_datetime("2026-08-27T20:30:55.0Z[UTC]")
        assert result == datetime(2026, 8, 27, 20, 30, 55, 0, tzinfo=timezone.utc)

    def test_new_serializer_nine_digit_nanos(self) -> None:
        # 9-digit nanos trimmed to 6-digit micros: "2025-05-23T13:10:55.123456789Z[UTC]"
        result = parse_gds_datetime("2025-05-23T13:10:55.123456789Z[UTC]")
        assert result == datetime(2025, 5, 23, 13, 10, 55, 123456, tzinfo=timezone.utc)

    def test_non_utc_zone(self) -> None:
        # Non-UTC zone with offset and bracket: "2025-01-02T12:39:46.745137+01:00[Europe/Berlin]"
        # Normalized to UTC: 12:39:46 +01:00 -> 11:39:46 UTC
        result = parse_gds_datetime("2025-01-02T12:39:46.745137+01:00[Europe/Berlin]")
        expected = datetime(2025, 1, 2, 11, 39, 46, 745137, tzinfo=timezone.utc)
        assert result == expected

    def test_non_utc_zone_nine_digit_nanos(self) -> None:
        # 9-digit nanos trimmed to 6, then normalized to UTC
        result = parse_gds_datetime("2025-01-02T12:39:46.745137123+01:00[Europe/Berlin]")
        expected = datetime(2025, 1, 2, 11, 39, 46, 745137, tzinfo=timezone.utc)
        assert result == expected

    def test_single_digit_fractional(self) -> None:
        result = parse_gds_datetime("2026-08-27T20:30:55.5Z[UTC]")
        assert result == datetime(2026, 8, 27, 20, 30, 55, 500000, tzinfo=timezone.utc)

    def test_the_original_bug_scenario(self) -> None:
        # The exact string that caused the original bug
        result = parse_gds_datetime("2026-08-27T20:30:55Z")
        assert result == datetime(2026, 8, 27, 20, 30, 55, tzinfo=timezone.utc)
