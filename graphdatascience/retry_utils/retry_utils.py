import logging
import typing

from tenacity import RetryCallState


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
                f"Retry of '{fn_name}', " f"attempt: {retry_state.attempt_number}",
            )

    return log_it
