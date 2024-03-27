import pytest
from pandas import DataFrame
from pyarrow.flight import FlightUnavailableError

from .conftest import CollectingQueryRunner
from graphdatascience.query_runner.arrow_query_runner import (
    ArrowQueryRunner,
    AuthFactory,
    AuthMiddleware,
)
from graphdatascience.server_version.server_version import ServerVersion


@pytest.mark.parametrize("server_version", [ServerVersion(2, 6, 0)])
def test_create(runner: CollectingQueryRunner) -> None:
    runner.set__mock_result(DataFrame([{"running": True, "listenAddress": "localhost:1234"}]))

    arrow_runner = ArrowQueryRunner.create(runner)

    assert isinstance(arrow_runner, ArrowQueryRunner)

    with pytest.raises(FlightUnavailableError, match=".+ failed to connect .+ ipv4:127.0.0.1:1234: .+"):
        arrow_runner._flight_client.list_actions()


@pytest.mark.parametrize("server_version", [ServerVersion(2, 6, 0)])
def test_return_fallback_when_arrow_is_not_running(runner: CollectingQueryRunner) -> None:
    runner.set__mock_result(DataFrame([{"running": False, "listenAddress": "localhost:1234"}]))

    arrow_runner = ArrowQueryRunner.create(runner)

    assert arrow_runner is runner


@pytest.mark.parametrize("server_version", [ServerVersion(2, 6, 0)])
def test_create_with_provided_connection(runner: CollectingQueryRunner) -> None:
    runner.set__mock_result(DataFrame([{"running": True, "listenAddress": "localhost:1234"}]))

    arrow_runner = ArrowQueryRunner.create(runner, connection_string_override="localhost:4321")

    assert isinstance(arrow_runner, ArrowQueryRunner)

    with pytest.raises(FlightUnavailableError, match=".+ failed to connect .+ ipv4:127.0.0.1:4321: .+"):
        arrow_runner._flight_client.list_actions()
