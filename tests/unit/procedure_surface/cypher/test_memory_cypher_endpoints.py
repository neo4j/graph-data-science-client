from __future__ import annotations

import pandas as pd

from graphdatascience.procedure_surface.api.memory_endpoints import MemoryListResult, MemorySummaryResult
from graphdatascience.procedure_surface.cypher.memory_cypher_endpoints import MemoryCypherEndpoints
from tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner


def test_summary_calls_procedure_and_parses_result() -> None:
    runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {"gds.memory.summary": pd.DataFrame([{"user": "alice", "totalGraphsMemory": 1024, "totalTasksMemory": 512}])},
    )
    endpoints = MemoryCypherEndpoints(runner)

    result = endpoints.summary()

    assert "gds.memory.summary" in runner.queries[-1]
    assert result == [MemorySummaryResult(user="alice", total_graphs_memory=1024, total_tasks_memory=512)]


def test_list_calls_procedure_and_parses_result() -> None:
    runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {"gds.memory.list": pd.DataFrame([{"user": "alice", "name": "g", "entity": "graph", "memoryInBytes": 2048}])},
    )
    endpoints = MemoryCypherEndpoints(runner)

    result = endpoints.list()

    assert "gds.memory.list" in runner.queries[-1]
    assert result == [MemoryListResult(user="alice", name="g", entity="graph", memory_in_bytes=2048)]
