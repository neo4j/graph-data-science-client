from pytest_mock import MockerFixture

from graphdatascience import ServerVersion
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.query_runner.query_mode import QueryMode
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.session_lifecycle_manager import SessionLifecycleManager
from graphdatascience.session.session_v2_endpoints import SessionV2Endpoints
from tests.unit.conftest import CollectingQueryRunner


def test_run_cypher_write(mocker: MockerFixture) -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        db_query_runner=None,
        session_lifecycle_manager=mocker.Mock(spec=SessionLifecycleManager),
        gds_version=v,
        v2_endpoints=mocker.Mock(),
        authenticated_arrow_client=mocker.Mock(),
    )

    gds.run_cypher("RETURN 1", params={"foo": 1}, mode=QueryMode.WRITE, database="bar", retryable=True)

    assert query_runner.last_query() == "RETURN 1"
    assert query_runner.last_params() == {"foo": 1}
    assert query_runner.run_args[-1] == {"custom_error": False, "db": "bar", "mode": QueryMode.WRITE, "retryable": True}


def test_run_cypher_read(mocker: MockerFixture) -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        db_query_runner=None,
        session_lifecycle_manager=mocker.Mock(spec=SessionLifecycleManager),
        gds_version=v,
        v2_endpoints=mocker.Mock(),
        authenticated_arrow_client=mocker.Mock(),
    )

    gds.run_cypher("RETURN 1", params={"foo": 1}, mode=QueryMode.READ, retryable=False)

    assert query_runner.last_query() == "RETURN 1"
    assert query_runner.last_params() == {"foo": 1}
    assert query_runner.run_args[-1] == {
        "custom_error": False,
        "db": None,
        "mode": QueryMode.READ,
        "retryable": False,
        "query_type": "user-direct",
    }


def test_verify_connectivity(mocker: MockerFixture) -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = mocker.Mock(spec=Neo4jQueryRunner)
    session_lifecycle_manager = mocker.Mock(spec=SessionLifecycleManager)
    v2_endpoints = mocker.Mock(spec=SessionV2Endpoints)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        db_query_runner=None,
        session_lifecycle_manager=session_lifecycle_manager,
        gds_version=v,
        v2_endpoints=v2_endpoints,
        authenticated_arrow_client=mocker.Mock(),
    )

    gds.verify_connectivity()

    session_lifecycle_manager.verify_health.assert_called_once()
    v2_endpoints.verify_session_connectivity.assert_called_once()
    v2_endpoints.verify_db_connectivity.assert_called_once()


def test_delete(mocker: MockerFixture) -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v)
    session_lifecycle_manager = mocker.Mock(spec=SessionLifecycleManager)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        db_query_runner=query_runner,
        session_lifecycle_manager=session_lifecycle_manager,
        gds_version=v,
        v2_endpoints=mocker.Mock(),
        authenticated_arrow_client=mocker.Mock(),
    )

    gds.delete()

    session_lifecycle_manager.delete.assert_called_once()
