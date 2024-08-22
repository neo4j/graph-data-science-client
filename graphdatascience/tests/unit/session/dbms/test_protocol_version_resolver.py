from neo4j.exceptions import Neo4jError
from pandas import DataFrame

from graphdatascience import ServerVersion
from graphdatascience.session.dbms.protocol_resolver import ProtocolVersionResolver
from graphdatascience.session.dbms.protocol_version import ProtocolVersion
from graphdatascience.tests.unit.conftest import CollectingQueryRunner


def test_protocol_versions() -> None:
    runner = CollectingQueryRunner(
        result_or_exception=DataFrame([{"version": "v1"}]), server_version=ServerVersion(1, 2, 3)
    )
    resolver = ProtocolVersionResolver(runner)

    assert resolver.protocol_versions_from_server() == [ProtocolVersion.V1]
    assert runner.last_query() == "CALL gds.session.dbms.protocol.version() YIELD version"
    assert runner.last_params() == {}


def test_protocol_versions_proc_missing() -> None:
    runner = CollectingQueryRunner(
        result_or_exception=Neo4jError("no such proc"), server_version=ServerVersion(1, 2, 3)
    )
    resolver = ProtocolVersionResolver(runner)

    assert resolver.protocol_versions_from_server() == [ProtocolVersion.V1]
    assert runner.last_query() == "CALL gds.session.dbms.protocol.version() YIELD version"
    assert runner.last_params() == {}
