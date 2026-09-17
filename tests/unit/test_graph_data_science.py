import pytest
from neo4j.exceptions import Neo4jError
from pyarrow.flight import ActionType
from pytest_mock import MockerFixture

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.versions import ServerVersion
from tests.unit.conftest import CollectingQueryRunner


@pytest.fixture
def supported_runner() -> CollectingQueryRunner:
    return CollectingQueryRunner(ServerVersion(2, 13, 0))


def procedure_not_found() -> Neo4jError:
    return Neo4jError._hydrate_neo4j(
        code="Neo.ClientError.Procedure.ProcedureNotFound",
        message="There is no procedure with the name `gds.debug.arrow` registered for this database instance.",
    )


def test_falls_back_to_cypher_if_arrow_procedure_not_registered(supported_runner: CollectingQueryRunner) -> None:
    supported_runner.add__mock_result("gds.debug.arrow", procedure_not_found())

    gds = GraphDataScience(supported_runner)

    try:
        assert gds._arrow_client is None

        assert gds.graph.list() == []
        assert "CALL gds.graph.list" in supported_runner.last_query()
    finally:
        gds.close()


def test_explicit_arrow_url_bypasses_arrow_discovery(
    supported_runner: CollectingQueryRunner, mocker: MockerFixture
) -> None:
    supported_runner.add__mock_result("gds.debug.arrow", procedure_not_found())
    mocker.patch(
        "graphdatascience.arrow_client.authenticated_flight_client.AuthenticatedArrowClient.list_actions_with_retry",
        return_value={ActionType("v1/admin", "GDS Admin")},
    )

    gds = GraphDataScience(supported_runner, arrow="localhost:8491")

    try:
        assert gds._arrow_client is not None
    finally:
        gds.close()
