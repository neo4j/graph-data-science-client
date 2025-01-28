import asyncio
import logging
import signal
from signal import Signals
from types import FrameType
from typing import Any, Optional


def cancel_on_signals(task: asyncio.Task[Any], signals: list[Signals]) -> None:
    def receive_signal(sig: int, frame: Optional[FrameType]) -> None:
        logging.warning(f"Received signal {sig}. Shutting down task.")
        task.cancel(f"Received signal {sig}")

    for sig in signals:
        signal.signal(sig, receive_signal)
