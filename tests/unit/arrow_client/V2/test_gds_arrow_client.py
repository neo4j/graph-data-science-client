from __future__ import annotations

from typing import Any, Callable

import pandas as pd
import pyarrow as pa
from pytest_mock import MockerFixture

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.gds_arrow_client import GdsArrowClient


class ImmediateRetryConfig:
    def decorator(
        self,
        operation_name: str,
        logger: Any,  # noqa: ANN401
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def apply(func: Callable[..., Any]) -> Callable[..., Any]:
            return func

        return apply


class StubWriter:
    def __init__(self) -> None:
        self.batches: list[pa.RecordBatch] = []

    def __enter__(self) -> StubWriter:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:  # noqa: ANN401
        return None

    def write_batch(self, batch: pa.RecordBatch) -> None:
        self.batches.append(batch)


class StubAckReader:
    def __init__(self, ack_log: list[str], label: str) -> None:
        self._ack_log = ack_log
        self._label = label

    def read(self) -> None:
        self._ack_log.append(self._label)


def test_upload_nodes_uses_one_do_put_per_dataframe_schema(mocker: MockerFixture) -> None:
    arrow_client = mocker.Mock(spec=AuthenticatedArrowClient)
    arrow_client._retry_config = ImmediateRetryConfig()

    schemas: list[pa.Schema] = []
    ack_reads: list[str] = []

    def do_put_with_retry(descriptor: Any, schema: pa.Schema) -> tuple[StubWriter, StubAckReader]:  # noqa: ARG001
        schemas.append(schema)
        label = f"stream-{len(schemas)}"
        return StubWriter(), StubAckReader(ack_reads, label)

    arrow_client.do_put_with_retry.side_effect = do_put_with_retry

    gds_arrow_client = GdsArrowClient(arrow_client)
    first = pd.DataFrame({"nodeId": [0, 1], "labels": ["A", "A"], "score": [1.0, 2.0]})
    second = pd.DataFrame({"nodeId": [2, 3], "labels": ["B", "B"], "rank": [5, 6]})

    gds_arrow_client.upload_nodes("job-123", [first, second], batch_size=1)

    assert [schema.names for schema in schemas] == [
        ["nodeId", "labels", "score"],
        ["nodeId", "labels", "rank"],
    ]
    assert ack_reads == ["stream-1", "stream-1", "stream-2", "stream-2"]


def test_upload_relationships_accumulates_progress_across_dataframes(mocker: MockerFixture) -> None:
    arrow_client = mocker.Mock(spec=AuthenticatedArrowClient)
    arrow_client._retry_config = ImmediateRetryConfig()
    arrow_client.do_put_with_retry.side_effect = (
        lambda descriptor, schema: (StubWriter(), StubAckReader([], "ack"))  # noqa: ARG005
    )

    progress: list[int] = []
    rel_a = pd.DataFrame({"sourceNodeId": [0, 1, 2], "targetNodeId": [1, 2, 3], "weight": [0.1, 0.2, 0.3]})
    rel_b = pd.DataFrame({"sourceNodeId": [3], "targetNodeId": [0], "flag": [1]})

    def progress_callback(num_rows: int) -> None:
        progress.append(num_rows)

    GdsArrowClient(arrow_client).upload_relationships(
        "job-123",
        [rel_a, rel_b],
        batch_size=2,
        progress_callback=progress_callback,
    )

    assert progress == [2, 1, 1]
