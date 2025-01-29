from __future__ import annotations

import logging
import signal
import threading
from abc import ABC, abstractmethod
from types import FrameType
from typing import Optional


class TerminationFlag(ABC):
    @abstractmethod
    def is_set(self) -> bool:
        pass

    @abstractmethod
    def set(self) -> None:
        pass

    def assert_running(self) -> None:
        if self.is_set():
            raise RuntimeError("Query has been terminated")

    @staticmethod
    def create(signals: Optional[list[signal.Signals]] = None) -> TerminationFlag:
        if signals is None:
            signals = [signal.SIGINT, signal.SIGTERM]

        if threading.current_thread() == threading.main_thread():
            return TerminationFlagImpl(signals)
        else:
            logging.debug("Cannot set terminationFlag for query runner in non-main thread")
            return TerminationFlagNoop()


class TerminationFlagImpl(TerminationFlag):
    def __init__(self, signals: list[signal.Signals]) -> None:
        self._event = threading.Event()

        def receive_signal(sig: int, frame: Optional[FrameType]) -> None:
            logging.debug(f"Received signal {sig}. Interrupting query.")
            self._event.set()

        for sig in signals:
            signal.signal(sig, receive_signal)

    def is_set(self) -> bool:
        return self._event.is_set()

    def set(self) -> None:
        self._event.set()

    def assert_running(self) -> None:
        if self.is_set():
            raise RuntimeError("Query has been terminated")


class TerminationFlagNoop(TerminationFlag):
    def __init__(self) -> None:
        pass

    def is_set(self) -> bool:
        return False

    def set(self) -> None:
        pass

    def assert_running(self) -> None:
        pass
