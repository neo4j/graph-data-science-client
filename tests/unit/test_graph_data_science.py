import neo4j
import pytest
from neo4j.exceptions import Neo4jError
from pyarrow.flight import ActionType
from pytest_mock import MockerFixture

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner import QueryMode
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
    mocker.patch(
        "graphdatascience.arrow_client.authenticated_flight_client.AuthenticatedArrowClient.list_actions_with_retry",
        return_value={ActionType("v1/admin", "GDS Admin")},
    )

    gds = GraphDataScience(supported_runner, arrow="localhost:8491")

    try:
        assert gds._arrow_client is not None
        assert all("gds.debug.arrow" not in query for query in supported_runner.queries)
    finally:
        gds.close()


def test_no_arrow_discovery_when_arrow_disabled(supported_runner: CollectingQueryRunner) -> None:
    gds = GraphDataScience(supported_runner, arrow=False)

    try:
        assert gds._arrow_client is None
        assert all("gds.debug.arrow" not in query for query in supported_runner.queries)
    finally:
        gds.close()


@pytest.mark.parametrize("endpoint", ["bolt+ssc://localhost:7687", "neo4j+ssc://localhost:7687", "NEO4J+SSC://host"])
def test_derives_disable_server_verification_from_ssc_uri(endpoint: str) -> None:
    assert GraphDataScience._derive_arrow_client_options(endpoint, None) == {"disable_server_verification": True}


def test_keeps_other_arrow_client_options_for_ssc_uri() -> None:
    options = {"call_timeout": 10}
    derived = GraphDataScience._derive_arrow_client_options("bolt+ssc://localhost:7687", options)

    assert derived == {"call_timeout": 10, "disable_server_verification": True}
    assert options == {"call_timeout": 10}


def test_does_not_override_explicit_server_verification_for_ssc_uri() -> None:
    options = {"disable_server_verification": False}
    assert GraphDataScience._derive_arrow_client_options("bolt+ssc://localhost:7687", options) == options


@pytest.mark.parametrize("endpoint", ["bolt://localhost:7687", "bolt+s://localhost:7687", "neo4j+s://localhost:7687"])
def test_does_not_derive_disable_server_verification_for_non_ssc_uri(endpoint: str) -> None:
    assert GraphDataScience._derive_arrow_client_options(endpoint, None) is None
    assert GraphDataScience._derive_arrow_client_options(endpoint, {"call_timeout": 10}) == {"call_timeout": 10}


def test_does_not_derive_disable_server_verification_for_query_runner(supported_runner: CollectingQueryRunner) -> None:
    assert GraphDataScience._derive_arrow_client_options(supported_runner, None) is None


def test_run_cypher_str_mode(supported_runner: CollectingQueryRunner) -> None:
    gds = GraphDataScience(supported_runner, arrow=False)

    try:
        gds.run_cypher("RETURN 1", mode="READ")

        assert supported_runner.last_run_args()["mode"] == QueryMode.READ
        assert supported_runner.last_run_args()["retryable"] is True
    finally:
        gds.close()


def test_run_cypher_lower_case_str_mode(supported_runner: CollectingQueryRunner) -> None:
    gds = GraphDataScience(supported_runner, arrow=False)

    try:
        gds.run_cypher("RETURN 1", mode="read")  # type: ignore[arg-type]

        assert supported_runner.last_run_args()["mode"] == QueryMode.READ
    finally:
        gds.close()


def test_run_cypher_invalid_mode(supported_runner: CollectingQueryRunner) -> None:
    gds = GraphDataScience(supported_runner, arrow=False)

    try:
        with pytest.raises(ValueError, match="Invalid query mode: 'reads'"):
            gds.run_cypher("RETURN 1", mode="reads")  # type: ignore[arg-type]
    finally:
        gds.close()


def test_run_cypher_auto_commit(supported_runner: CollectingQueryRunner) -> None:
    gds = GraphDataScience(supported_runner, arrow=False)

    try:
        gds.run_cypher("RETURN 1", params={"foo": 1}, database="bar", auto_commit=True)

        assert supported_runner.last_query() == "RETURN 1"
        assert supported_runner.last_params() == {"foo": 1}
        assert supported_runner.last_run_args() == {
            "db": "bar",
            "mode": QueryMode.WRITE,
            "custom_error": False,
            "retryable": False,
            "query_type": "user-direct",
        }
    finally:
        gds.close()


def test_run_cypher_uses_transactional_retries_by_default(supported_runner: CollectingQueryRunner) -> None:
    gds = GraphDataScience(supported_runner, arrow=False)

    try:
        gds.run_cypher("RETURN 1")

        assert supported_runner.last_run_args()["retryable"] is True
    finally:
        gds.close()


def test_db_driver_returns_query_runner_driver(mocker: MockerFixture) -> None:
    driver = mocker.Mock(spec=neo4j.Driver)
    runner = CollectingQueryRunner(ServerVersion(2, 13, 0), db_driver=driver)
    gds = GraphDataScience(runner, arrow=False)

    try:
        assert gds.db_driver() is driver
    finally:
        gds.close()
