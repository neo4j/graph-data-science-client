import signal
from types import FrameType
from typing import Optional

from tenacity import RetryCallState, retry_base


class retry_unless_signal(retry_base):
    """Retries unless one of the given signals is raised."""

    def __init__(self, signals: list[signal.Signals]) -> None:
        self.signal_received = False

        def receive_signal(sig: int, frame: Optional[FrameType]) -> None:
            self.signal_received = True

        for sig in signals:
            signal.signal(sig, receive_signal)

    def __call__(self, retry_state: RetryCallState) -> bool:
        return not self.signal_received
