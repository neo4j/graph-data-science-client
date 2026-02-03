from typing import Protocol


class ProgressCallback(Protocol):
    def __call__(self, num_rows: int) -> None: ...
