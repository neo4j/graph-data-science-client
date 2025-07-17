import json
from typing import Any

from pyarrow._flight import Result


class ArrowTestResult(Result):  # type:ignore
    def __init__(self, body: dict[str, Any]):
        self._body = json.dumps(body).encode()

    @property
    def body(self) -> Any:
        class MockBody:
            def __init__(self, data: bytes):
                self._data = data

            def to_pybytes(self) -> bytes:
                return self._data

        return MockBody(self._body)
