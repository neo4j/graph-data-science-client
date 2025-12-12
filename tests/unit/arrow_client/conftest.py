import pytest
from pyarrow._flight import FlightInternalError, FlightTimedOutError, FlightUnavailableError

from graphdatascience.retry_utils.retry_config import RetryConfigV2, StopConfig


@pytest.fixture
def retry_config_v2() -> RetryConfigV2:
    return RetryConfigV2(
        retryable_exceptions=[
            FlightTimedOutError,
            FlightUnavailableError,
            FlightInternalError,
        ],
        stop_config=StopConfig(after_delay=10, after_attempt=5),
        wait_config=None,  # No wait for tests. Makes them faster
    )
