from types import TracebackType
from typing import Generic, NamedTuple, Optional, Type, TypeVar

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import Graph

RESULT = TypeVar("RESULT", bound=BaseResult)


class GraphWithResult(NamedTuple, Generic[RESULT]):
    """
    A result object wrapping a Graph object and a result.

    Can be used as a context manager to ensure the graph is dropped after use.
    """

    graph: Graph
    result: RESULT

    def __enter__(self) -> Graph:
        return self.graph

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.graph.drop()
