from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.cypher.config_cypher_endpoints import ConfigCypherEndpoints


@pytest.fixture
def config_endpoints(query_runner: QueryRunner) -> Generator[ConfigCypherEndpoints, None, None]:
    yield ConfigCypherEndpoints(query_runner)


def test_defaults_set_and_list(config_endpoints: ConfigCypherEndpoints) -> None:
    config_endpoints.defaults.set("test.key", 6)

    defaults = config_endpoints.defaults.list()

    assert ("test.key", 6) in defaults.items()


def test_defaults_set_with_username(config_endpoints: ConfigCypherEndpoints) -> None:
    config_endpoints.defaults.set("test.user.key", "user_value", username="testuser")
    config_endpoints.defaults.set("test.user.key", "user2_value", username="testuser2")

    user_defaults = config_endpoints.defaults.list(username="testuser")

    assert ("test.user.key", "user_value") in user_defaults.items()
    assert ("test.user.key", "user2_value") not in user_defaults.items()


def test_defaults_list_by_key(config_endpoints: ConfigCypherEndpoints) -> None:
    config_endpoints.defaults.set("test.specific.key", "specific_value")

    specific_defaults = config_endpoints.defaults.list(key="test.specific.key")

    assert specific_defaults == {"test.specific.key": "specific_value"}


def test_limits_set_and_list(config_endpoints: ConfigCypherEndpoints) -> None:
    config_endpoints.limits.set("test.key", 6)

    limits = config_endpoints.limits.list()

    assert ("test.key", 6) in limits.items()


def test_limits_set_with_username(config_endpoints: ConfigCypherEndpoints) -> None:
    config_endpoints.limits.set("test.user.key", 1, username="testuser")
    config_endpoints.limits.set("test.user.key", 2, username="testuser2")

    user_limits = config_endpoints.limits.list(username="testuser")

    assert ("test.user.key", 1) in user_limits.items()
    assert ("test.user.key", 2) not in user_limits.items()


def test_limits_list_by_key(config_endpoints: ConfigCypherEndpoints) -> None:
    config_endpoints.limits.set("test.specific.key", 42)

    specific_limits = config_endpoints.limits.list(key="test.specific.key")

    assert specific_limits == {"test.specific.key": 42}


def test_config_endpoints_properties(config_endpoints: ConfigCypherEndpoints) -> None:
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
