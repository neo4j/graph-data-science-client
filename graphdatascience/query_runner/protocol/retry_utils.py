import logging
import signal
import typing
from types import FrameType
from typing import Optional

from tenacity import RetryCallState, retry_base


class retry_unless_signal(retry_base):
    """Retries unless one of the given signals is raised."""

    def __init__(self, signals: list[signal.Signals]) -> None:
        self.signal_received = False

        def receive_signal(sig: int, frame: Optional[FrameType]) -> None:
            logging.debug(f"Received signal {sig}. Interrupting retry.")
            self.signal_received = True

        try:
            for sig in signals:
                signal.signal(sig, receive_signal)
        except ValueError as e:
            # signal.signal() can raise ValueError if this is not called in the main thread (such as when an algorithm is called in a ThreadPool)
            logging.debug(f"Cannot set signal handler for retries {e}")

    def __call__(self, retry_state: RetryCallState) -> bool:
        return not self.signal_received


def before_log(
    fn_name: str,
    logger: logging.Logger,
    log_level: int,
    sec_format: str = "%0.3f",
) -> typing.Callable[[RetryCallState], None]:
    def log_it(retry_state: RetryCallState) -> None:
        logger.log(
            log_level,
            f"Retry of '{fn_name}', " f"attempt: {retry_state.attempt_number}",
        )

    return log_it
