from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from typing import Any, Callable

from pydantic import BaseModel
from tenacity import WrappedFn, retry
from tenacity.retry import retry_always, retry_any, retry_base, retry_if_exception_type
from tenacity.stop import stop_after_attempt, stop_after_delay, stop_any, stop_base
from tenacity.wait import wait_base, wait_exponential

from graphdatascience.retry_utils.retry_utils import before_log


@dataclass(frozen=True, repr=True)
class RetryConfig:
    stop: stop_base
    wait: wait_base
    retry: retry_base


class RetryConfigV2(BaseModel):
    """Retry configuration which can be serialized/deserialized."""

    retryable_exceptions: list[type[BaseException]] | None = None
    stop_config: StopConfig | None = None
    wait_config: ExponentialWaitConfig | None = None

    def decorator(
        self,
        logger: logging.Logger | None = None,
        log_level: int = logging.DEBUG,
        operation_name: str = "Operation",
    ) -> Callable[[WrappedFn], WrappedFn]:
        """
        Create a tenacity retry decorator configured with this retry config.

        Parameters
        ----------
        logger
            Logger to use for retry logging. If None, no logging is performed.
        log_level
            Logging level to use (e.g., logging.DEBUG, logging.INFO).
        operation_name
            Name of the operation being retried, used in log messages.

        Returns
        -------
        Callable
            A decorator that can be applied to functions to add retry behavior.

        Examples
        --------
        >>> config = RetryConfigV2(
        ...     retryable_exceptions=[ValueError, KeyError],
        ...     stop_config=StopConfig(after_attempt=3),
        ...     wait_config=ExponentialWaitConfig(multiplier=1, min=1, max=10)
        ... )
        >>> @config.create_retry_decorator()
        ... def my_function():
        ...     # function logic here
        ...     pass
        """
        kwargs: dict[str, Any] = {}

        # Add retry condition
        retry_condition = self.tenacity_retry()
        if retry_condition is not None:
            kwargs["retry"] = retry_condition

        # Add stop condition
        stop_condition = self.tenacity_stop()
        if stop_condition is not None:
            kwargs["stop"] = stop_condition

        # Add wait strategy
        wait_strategy = self.tenacity_wait()
        if wait_strategy is not None:
            kwargs["wait"] = wait_strategy

        # Add logging if logger is provided
        if logger is not None:
            kwargs["before"] = before_log(operation_name, logger, log_level)

        return retry(**kwargs, reraise=True)

    def tenacity_wait(self) -> wait_base | None:
        if self.wait_config is None:
            return None

        return self.wait_config.to_tenacity()

    def tenacity_stop(self) -> stop_base | None:
        if self.stop_config is None:
            return None

        return self.stop_config.to_tenacity()

    def tenacity_retry(self) -> retry_base | None:
        if self.retryable_exceptions is None:
            return retry_always

        retries = [retry_if_exception_type(exc) for exc in self.retryable_exceptions]
        if len(retries) == 1:
            return retries[0]

        return retry_any(*retries)


class StopConfig(BaseModel):
    after_attempt: int | None = None
    after_delay: int | None = None

    def to_tenacity(self) -> stop_base | None:
        stops: list[stop_base] = []
        if self.after_attempt is not None:
            stops.append(stop_after_attempt(self.after_attempt))
        if self.after_delay is not None:
            stops.append(stop_after_delay(self.after_delay))

        if not stops:
            return None
        if len(stops) == 1:
            return stops[0]

        return stop_any(*stops)


class ExponentialWaitConfig(BaseModel):
    multiplier: float = 1
    min: int = 0
    max: float | int = sys.maxsize / 2
    exp_base: int = 2

    def to_tenacity(self) -> wait_base | None:
        return wait_exponential(
            multiplier=self.multiplier,
            min=self.min,
            max=self.max,
        )
