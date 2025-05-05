import logging
import typing
from concurrent.futures import Future

from tenacity import RetryCallState, retry_base


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


class retry_until_future(retry_base):
    def __init__(
        self,
        future: Future[typing.Any],
    ):
        self._future = future

    def __call__(self, retry_state: "RetryCallState") -> bool:
        return not self._future.done()
