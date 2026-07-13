from __future__ import annotations

from graphdatascience.procedure_surface.api.memory_endpoints import (
    MemoryEndpoints,
    MemoryListResult,
    MemorySummaryResult,
)
from graphdatascience.query_runner.query_runner import QueryRunner


class MemoryCypherEndpoints(MemoryEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def summary(self) -> list[MemorySummaryResult]:
        result = self._query_runner.call_procedure(endpoint="gds.memory.summary")
        return [MemorySummaryResult(**row) for _, row in result.iterrows()]

    def list(self) -> list[MemoryListResult]:
        result = self._query_runner.call_procedure(endpoint="gds.memory.list")
        return [MemoryListResult(**row) for _, row in result.iterrows()]
