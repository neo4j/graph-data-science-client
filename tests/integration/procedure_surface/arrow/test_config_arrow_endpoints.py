from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.config_arrow_endpoints import ConfigArrowEndpoints


@pytest.fixture
def config_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[ConfigArrowEndpoints, None, None]:
    yield ConfigArrowEndpoints(arrow_client)


def test_defaults_set_and_list(config_endpoints: ConfigArrowEndpoints) -> None:
    config_endpoints.defaults.set("test.key", 6)

    defaults = config_endpoints.defaults.list()

    assert ("test.key", 6) in defaults.items()


def test_defaults_list_by_key(config_endpoints: ConfigArrowEndpoints) -> None:
    config_endpoints.defaults.set("test.specific.key", "specific_value")

    specific_defaults = config_endpoints.defaults.list(key="test.specific.key")

    assert specific_defaults == {"test.specific.key": "specific_value"}


def test_limits_set_and_list(config_endpoints: ConfigArrowEndpoints) -> None:
    config_endpoints.limits.set("test.key", 6)

    limits = config_endpoints.limits.list()

    assert ("test.key", 6) in limits.items()


def test_limits_list_by_key(config_endpoints: ConfigArrowEndpoints) -> None:
    config_endpoints.limits.set("test.specific.key", 42)

    specific_limits = config_endpoints.limits.list(key="test.specific.key")

    assert specific_limits == {"test.specific.key": 42}


def test_config_endpoints_properties(config_endpoints: ConfigArrowEndpoints) -> None:
    """Test that the config endpoints have the required properties."""
    assert hasattr(config_endpoints, "defaults")
    assert hasattr(config_endpoints, "limits")

    # Verify the properties return the correct endpoint types
    defaults = config_endpoints.defaults
    limits = config_endpoints.limits

    assert hasattr(defaults, "set")
    assert hasattr(defaults, "list")
    assert hasattr(limits, "set")
    assert hasattr(limits, "list")
