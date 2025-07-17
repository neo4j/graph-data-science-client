import json
from typing import Any, Iterator

from pyarrow._flight import Result


def deserialize_single(input_stream: Iterator[Result]) -> dict[str, Any]:
    rows = deserialize(input_stream)
    if len(rows) != 1:
        raise ValueError(f"Expected exactly one result, got {len(rows)}")

    return rows[0]


def deserialize(input_stream: Iterator[Result]) -> list[dict[str, Any]]:
    def deserialize_row(row: Result):  # type:ignore
        return json.loads(row.body.to_pybytes().decode())

    return [deserialize_row(row) for row in list(input_stream)]
