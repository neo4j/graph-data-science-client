import pytest
from pyarrow.flight import FlightUnavailableError

from graphdatascience.query_runner.arrow_info import ArrowInfo
from graphdatascience.query_runner.arrow_query_runner import ArrowQueryRunner
from graphdatascience.server_version.server_version import ServerVersion

from .conftest import CollectingQueryRunner


@pytest.mark.parametrize("server_version", [ServerVersion(2, 6, 0)])
def test_create(runner: CollectingQueryRunner) -> None:
    arrow_info = ArrowInfo(listenAddress="localhost:1234", enabled=True, running=True, versions=[])
    arrow_runner = ArrowQueryRunner.create(runner, arrow_info)

    assert isinstance(arrow_runner, ArrowQueryRunner)

    with pytest.raises(FlightUnavailableError, match=".+ failed to connect .+ ipv4:127.0.0.1:1234: .+"):
        arrow_runner._gds_arrow_client.send_action("TEST", {})


@pytest.mark.parametrize("server_version", [ServerVersion(2, 6, 0)])
def test_return_fallback_when_arrow_is_not_enabled(runner: CollectingQueryRunner) -> None:
    arrow_info = ArrowInfo(listenAddress="localhost:1234", enabled=False, running=False, versions=[])

    with pytest.raises(ValueError, match="Arrow is not enabled"):
        ArrowQueryRunner.create(runner, arrow_info)


@pytest.mark.parametrize("server_version", [ServerVersion(2, 6, 0)])
def test_create_with_provided_connection(runner: CollectingQueryRunner) -> None:
    arrow_info = ArrowInfo(listenAddress="localhost:1234", enabled=True, running=True, versions=[])

    arrow_runner = ArrowQueryRunner.create(runner, arrow_info, connection_string_override="localhost:4321")

    assert isinstance(arrow_runner, ArrowQueryRunner)

    with pytest.raises(FlightUnavailableError, match=".+ failed to connect .+ ipv4:127.0.0.1:4321: .+"):
        arrow_runner._gds_arrow_client.send_action("TEST", {})
