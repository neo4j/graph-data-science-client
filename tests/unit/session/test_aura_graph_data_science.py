from pandas import DataFrame
from pytest_mock import MockerFixture

from graphdatascience import ServerVersion
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.query_runner.query_mode import QueryMode
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.session_lifecycle_manager import Noop, SessionLifecycleManager
from tests.unit.conftest import CollectingQueryRunner


def test_run_cypher_write(mocker: MockerFixture) -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v, {"version": DataFrame.from_dict({"version": ["v3"]})})
    gds = AuraGraphDataScience(
        mocker.Mock(),
        db_query_runner=query_runner,
        session_lifecycle_manager=Noop(),
    )

    gds.run_cypher("RETURN 1", params={"foo": 1}, mode=QueryMode.WRITE, database="bar", retryable=True)

    assert query_runner.last_query() == "RETURN 1"
    assert query_runner.last_params() == {"foo": 1}
    assert query_runner.run_args[-1] == {"custom_error": False, "db": "bar", "mode": QueryMode.WRITE, "retryable": True}


def test_run_cypher_read(mocker: MockerFixture) -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v, {"version": DataFrame.from_dict({"version": ["v3"]})})
    gds = AuraGraphDataScience(
        mocker.Mock(),
        db_query_runner=query_runner,
        session_lifecycle_manager=Noop(),
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
    arrow_client = mocker.Mock(spec=AuthenticatedArrowClient)
    session_lifecycle_manager = mocker.Mock(spec=SessionLifecycleManager)
    gds = AuraGraphDataScience(
        arrow_client,
        None,
        session_lifecycle_manager,
    )

    gds.verify_connectivity()

    session_lifecycle_manager.verify_health.assert_called_once()
    arrow_client.request_token.assert_called_once()


def test_delete(mocker: MockerFixture) -> None:
    arrow_client = mocker.Mock(spec=AuthenticatedArrowClient)
    session_lifecycle_manager = mocker.Mock(spec=SessionLifecycleManager)
    gds = AuraGraphDataScience(
        arrow_client,
        None,
        session_lifecycle_manager,
    )

    gds.delete()

    session_lifecycle_manager.delete.assert_called_once()
