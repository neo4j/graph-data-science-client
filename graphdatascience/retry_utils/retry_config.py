from dataclasses import dataclass

from tenacity.retry import retry_base
from tenacity.stop import stop_base
from tenacity.wait import wait_base


@dataclass(frozen=True, repr=True)
class RetryConfig:
    stop: stop_base
    wait: wait_base
    retry: retry_base
