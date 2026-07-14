from __future__ import annotations

from abc import ABC, abstractmethod

from graphdatascience.graph.graph_info import GraphInfo, GraphInfoWithDegrees


class GraphBackend(ABC):
    @abstractmethod
    def graph_info(self) -> GraphInfoWithDegrees:
        pass

    @abstractmethod
    def exists(self) -> bool:
        pass

    @abstractmethod
    def drop(self, fail_if_missing: bool = True) -> GraphInfo | None:
        pass
