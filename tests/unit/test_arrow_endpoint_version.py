from unittest import mock

import pytest
from pyarrow.flight import ActionType

from graphdatascience.arrow_client.arrow_endpoint_version import (
    ArrowEndpointVersion,
    UnsupportedArrowEndpointVersion,
)
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient


def test_prefix() -> None:
    assert ArrowEndpointVersion.V1.prefix() == "v1/"


def test_check_version_compatibility() -> None:
    arrow_client_mock = mock.Mock(AuthenticatedArrowClient)
    arrow_client_mock.list_actions.return_value = [
        ActionType("v2/foo", ""),
        ActionType("v2/bar", ""),
        ActionType("v3/baz", ""),
    ]

    ArrowEndpointVersion.check_version_compatibility({ArrowEndpointVersion.V2}, arrow_client_mock)

    with pytest.raises(UnsupportedArrowEndpointVersion) as e:
        ArrowEndpointVersion.check_version_compatibility({ArrowEndpointVersion.V1}, arrow_client_mock)
    assert "Unsupported" in str(e.value)
    assert "v1" in str(e.value)
