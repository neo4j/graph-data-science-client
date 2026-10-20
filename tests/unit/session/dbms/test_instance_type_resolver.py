import pytest
from neo4j.exceptions import Neo4jError
from pandas import DataFrame

from graphdatascience import ServerVersion
from graphdatascience.session.dbms.instance_type_resolver import InstanceTypeResolver
from graphdatascience.session.instance_type import InstanceType, UnknownDatabaseType
from tests.unit.conftest import CollectingQueryRunner

_QUERY = "SHOW DATABASES YIELD name, type, home WHERE name = $database OR (home AND $database IS NULL) RETURN type"


def _runner(database_type: str | Exception) -> CollectingQueryRunner:
    result: DataFrame | Exception
    if isinstance(database_type, Exception):
        result = database_type
    else:
        result = DataFrame([{"type": database_type}])

    return CollectingQueryRunner(result_mock=result, server_version=ServerVersion(1, 2, 3))


def test_resolves_virtual_graph() -> None:
    runner = _runner("virtual graph")

    assert InstanceTypeResolver(runner).resolve() is InstanceType.VIRTUAL_GRAPH
    assert runner.last_query() == _QUERY
    assert runner.last_params() == {"database": "dummy"}


def test_resolves_standard_as_database() -> None:
    assert InstanceTypeResolver(_runner("standard")).resolve() is InstanceType.DATABASE


def test_normalizes_reported_type() -> None:
    assert InstanceTypeResolver(_runner(" Virtual Graph ")).resolve() is InstanceType.VIRTUAL_GRAPH


def test_without_explicit_database_passes_no_filter() -> None:
    runner = _runner("virtual graph")
    runner.set_database("")

    assert InstanceTypeResolver(runner).resolve() is InstanceType.VIRTUAL_GRAPH
    assert runner.last_params() == {"database": None}


def test_resolves_composite() -> None:
    assert InstanceTypeResolver(_runner("composite")).resolve() is InstanceType.COMPOSITE


def test_unknown_type_raises() -> None:
    with pytest.raises(UnknownDatabaseType, match="Unknown database type `something else`"):
        InstanceTypeResolver(_runner("something else")).resolve()


def test_no_matching_database_falls_back_to_database() -> None:
    runner = CollectingQueryRunner(result_mock=DataFrame(), server_version=ServerVersion(1, 2, 3))

    assert InstanceTypeResolver(runner).resolve() is InstanceType.DATABASE


def test_failed_query_falls_back_to_database() -> None:
    assert InstanceTypeResolver(_runner(Neo4jError("no such command"))).resolve() is InstanceType.DATABASE
