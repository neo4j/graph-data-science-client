import pytest

from graphdatascience.query_runner import QueryMode


def test_of_passes_members_through() -> None:
    assert QueryMode.of(QueryMode.READ) is QueryMode.READ
    assert QueryMode.of(QueryMode.WRITE) is QueryMode.WRITE


def test_of_casts_plain_strings() -> None:
    assert QueryMode.of("READ") is QueryMode.READ
    assert QueryMode.of("WRITE") is QueryMode.WRITE


def test_of_accepts_lower_case_strings() -> None:
    assert QueryMode.of("read") is QueryMode.READ
    assert QueryMode.of("write") is QueryMode.WRITE


def test_of_rejects_invalid_strings() -> None:
    with pytest.raises(ValueError, match="Invalid query mode: 'reads'"):
        QueryMode.of("reads")
