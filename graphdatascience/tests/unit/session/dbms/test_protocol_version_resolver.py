import pytest
from neo4j.exceptions import Neo4jError
from pandas import DataFrame

from graphdatascience import ServerVersion
from graphdatascience.session.dbms.protocol_resolver import (
    ProtocolVersionResolver,
    UnsupportedProtocolVersion,
)
from graphdatascience.session.dbms.protocol_version import ProtocolVersion
from graphdatascience.tests.unit.conftest import CollectingQueryRunner


def test_protocol_versions() -> None:
    runner = CollectingQueryRunner(
        result_or_exception=DataFrame([{"version": "v1"}]), server_version=ServerVersion(1, 2, 3)
    )
    resolver = ProtocolVersionResolver(runner)

    assert resolver.resolve() == ProtocolVersion.V1
    assert runner.last_query() == "CALL gds.session.dbms.protocol.version() YIELD version"
    assert runner.last_params() == {}


def test_protocol_versions_caching() -> None:
    runner = CollectingQueryRunner(
        result_or_exception=DataFrame([{"version": "v2"}]), server_version=ServerVersion(1, 2, 3)
    )
    resolver = ProtocolVersionResolver(runner)

    resolver.resolve()
    resolver.resolve()
    resolver.resolve()
    assert runner.queries == ["CALL gds.session.dbms.protocol.version() YIELD version"]


def test_protocol_versions_proc_missing() -> None:
    runner = CollectingQueryRunner(
        result_or_exception=Neo4jError("no such proc"), server_version=ServerVersion(1, 2, 3)
    )
    resolver = ProtocolVersionResolver(runner)

    assert resolver.resolve() == ProtocolVersion.V1
    assert runner.last_query() == "CALL gds.session.dbms.protocol.version() YIELD version"
    assert runner.last_params() == {}


def test_protocol_versions_multiple_matching() -> None:
    runner = CollectingQueryRunner(
        result_or_exception=DataFrame([{"version": "v1"}, {"version": "v2"}]), server_version=ServerVersion(1, 2, 3)
    )
    resolver = ProtocolVersionResolver(runner)

    assert resolver.resolve() == ProtocolVersion.V2
    assert runner.last_query() == "CALL gds.session.dbms.protocol.version() YIELD version"
    assert runner.last_params() == {}


def test_protocol_versions_newer_dbms() -> None:
    runner = CollectingQueryRunner(
        result_or_exception=DataFrame([{"version": "v1"}, {"version": "v2"}, {"version": "v100"}]),
        server_version=ServerVersion(1, 2, 3),
    )
    resolver = ProtocolVersionResolver(runner)

    assert resolver.resolve() == ProtocolVersion.V2
    assert runner.last_query() == "CALL gds.session.dbms.protocol.version() YIELD version"
    assert runner.last_params() == {}


def test_protocol_versions_newer_dbms_no_matching() -> None:
    runner = CollectingQueryRunner(
        result_or_exception=DataFrame([{"version": "v100"}]), server_version=ServerVersion(1, 2, 3)
    )
    resolver = ProtocolVersionResolver(runner)

    with pytest.raises(
        UnsupportedProtocolVersion,
        match=r"The GDS Python Client does not support any procedure protocol version in the server",
    ):
        resolver.resolve()
