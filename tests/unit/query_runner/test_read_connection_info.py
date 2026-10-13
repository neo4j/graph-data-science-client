from unittest.mock import Mock

import neo4j
from neo4j import Address, RoutingControl

from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.versions import SemanticVersion

_NEO4J_DRIVER_VERSION = SemanticVersion.from_string(neo4j.__version__)


def _resolved_address(ip: str, port: int, host_name: str) -> Address:
    """Mirrors the driver's ResolvedAddress: `.host` is the resolved IP, `_unresolved` keeps the hostname."""
    if _NEO4J_DRIVER_VERSION >= SemanticVersion(6, 0, 0):
        from neo4j._addressing import ResolvedAddress
    else:
        from neo4j.addressing import ResolvedAddress  # type: ignore[attr-defined, no-redef]

    return ResolvedAddress((ip, port), host_name=host_name)


def _runner_connecting_to(address: Address) -> tuple[Neo4jQueryRunner, Mock]:
    summary = Mock()
    summary.server.address = address
    driver = Mock()
    driver.execute_query.return_value.summary = summary

    return Neo4jQueryRunner(driver=driver, protocol="bolt"), driver


def test_read_connection_info_returns_unresolved_hostname() -> None:
    runner, _ = _runner_connecting_to(_resolved_address("34.117.9.1", 7687, "abc.databases.neo4j.io"))

    info = runner.read_connection_info()

    assert info.host == "abc.databases.neo4j.io"
    assert info.port == 7687


def test_read_connection_info_falls_back_to_given_address() -> None:
    address = Address(("plain-host", 7687))
    runner, _ = _runner_connecting_to(address)

    assert runner.read_connection_info() == address


def test_read_connection_info_discovers_member_with_read_query() -> None:
    runner, driver = _runner_connecting_to(Address(("host", 7687)))

    runner.read_connection_info()

    driver.execute_query.assert_called_once_with("RETURN 1", database_=None, routing_=RoutingControl.READ)
