import logging
import typing

import tenacity.wait
from tenacity import RetryCallState, wait_chain, wait_fixed


def before_log(
    fn_name: str,
    logger: logging.Logger,
    log_level: int,
    sec_format: str = "%0.3f",
) -> typing.Callable[[RetryCallState], None]:
    def log_it(retry_state: RetryCallState) -> None:
        if retry_state.attempt_number > 1:  # only log on actual retry
            logger.log(
                log_level,
                f"Retry of '{fn_name}', attempt: {retry_state.attempt_number}",
            )

    return log_it


def job_wait_strategy() -> tenacity.wait.wait_base:
    # Wait for 0.02 s in the very beginning (to speed up tests)
    # Wait for 0.1 s in the first 10 seconds
    # Then increase exponentially to a max of 5 seconds
    return wait_chain(
        *[wait_fixed(0.02)]
        + [wait_fixed(0.1) for j in range(100)]
        + [wait_fixed(1), wait_fixed(2), wait_fixed(4), wait_fixed(5)]
    )
