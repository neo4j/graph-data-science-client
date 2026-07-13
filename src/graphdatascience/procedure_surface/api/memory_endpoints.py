from __future__ import annotations

from abc import ABC, abstractmethod

from graphdatascience.procedure_surface.api.base_result import BaseResult


class MemoryEndpoints(ABC):
    @abstractmethod
    def summary(self) -> list[MemorySummaryResult]:
        """
        Return a per-user summary of the memory used by GDS graphs and running tasks.

        Not available in AuraDS.

        Returns
        -------
        list[MemorySummaryResult]
            The aggregated graph and task memory used, one entry per user.
        """

    @abstractmethod
    def list(self) -> list[MemoryListResult]:
        """
        List the memory used by GDS per graph and per running task.

        Not available in AuraDS.

        Returns
        -------
        list[MemoryListResult]
            The memory used by each graph and task entity, one entry per entity.
        """


class MemorySummaryResult(BaseResult):
    user: str
    total_graphs_memory: int
    total_tasks_memory: int


class MemoryListResult(BaseResult):
    user: str
    name: str
    entity: str
    memory_in_bytes: int
