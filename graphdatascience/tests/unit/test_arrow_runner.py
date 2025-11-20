import pytest
from pyarrow.flight import FlightUnavailableError

from graphdatascience.query_runner.arrow_info import ArrowInfo
from graphdatascience.query_runner.arrow_query_runner import ArrowQueryRunner
from graphdatascience.retry_utils.retry_config import RetryConfigV2, StopConfig

from ...arrow_client.arrow_endpoint_version import ArrowEndpointVersion
from .conftest import CollectingQueryRunner


def test_create(runner: CollectingQueryRunner) -> None:
    arrow_info = ArrowInfo(
        listenAddress="localhost:1234", enabled=True, running=True, versions=[ArrowEndpointVersion.V1.version()]
    )
    retry_config = RetryConfigV2(
        retryable_exceptions=[],
        stop_config=StopConfig(after_attempt=1),
    )

    arrow_runner = ArrowQueryRunner.create(runner, arrow_info, retry_config=retry_config)

    assert isinstance(arrow_runner, ArrowQueryRunner)

    with pytest.raises(FlightUnavailableError, match=".+ failed to connect .+ ipv4:127.0.0.1:1234: .+"):
        arrow_runner._gds_arrow_client._send_action("TEST", {})


def test_return_fallback_when_arrow_is_not_enabled(runner: CollectingQueryRunner) -> None:
    arrow_info = ArrowInfo(listenAddress="localhost:1234", enabled=False, running=False, versions=[])

    with pytest.raises(ValueError, match="Arrow is not enabled"):
        ArrowQueryRunner.create(runner, arrow_info)


def test_create_with_provided_connection(runner: CollectingQueryRunner) -> None:
    arrow_info = ArrowInfo(
        listenAddress="localhost:1234", enabled=True, running=True, versions=[ArrowEndpointVersion.V1.version()]
    )

    arrow_runner = ArrowQueryRunner.create(runner, arrow_info, connection_string_override="localhost:4321")

    assert isinstance(arrow_runner, ArrowQueryRunner)

    with pytest.raises(FlightUnavailableError, match=".+ failed to connect .+ ipv4:127.0.0.1:4321: .+"):
        arrow_runner._gds_arrow_client._send_action("TEST", {})
