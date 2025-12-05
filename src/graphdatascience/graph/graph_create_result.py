from __future__ import annotations

from types import TracebackType
from typing import Any, NamedTuple, Type

from pandas import Series

from .graph_object import Graph


class GraphCreateResult(NamedTuple):
    """
    A result object returned by endpoints which create graphs.
    """

    graph: Graph
    result: Series[Any]

    def __enter__(self: GraphCreateResult) -> Graph:
        return self.graph

    def __exit__(
        self,
        exception_type: Type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.graph.drop()
