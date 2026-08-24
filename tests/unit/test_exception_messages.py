import pytest

from graphdatascience.arrow_client.arrow_endpoint_version import (
    ArrowEndpointVersion,
    UnsupportedArrowEndpointVersion,
)
from graphdatascience.error.standalone_session_error import NotAvailableInStandaloneSessions


def test_not_available_in_standalone_sessions_message() -> None:
    with pytest.raises(NotAvailableInStandaloneSessions) as e:
        raise NotAvailableInStandaloneSessions("Running Cypher queries")

    assert str(e.value) == "Running Cypher queries is not available in standalone sessions"


def test_unsupported_arrow_endpoint_version_message() -> None:
    with pytest.raises(UnsupportedArrowEndpointVersion) as e:
        raise UnsupportedArrowEndpointVersion({ArrowEndpointVersion.V2}, {"v1"})

    msg = str(e.value)
    assert "please update the `graphdatascience` package" in msg
    assert "v2" in msg
    assert "v1" in msg
