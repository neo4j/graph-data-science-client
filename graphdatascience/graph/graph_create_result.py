from __future__ import annotations

from types import TracebackType
from typing import Any, NamedTuple, Optional, Type

from pandas import Series

from .graph_object import Graph


class GraphCreateResult(NamedTuple):
    """
    A result object returned by endpoints which create graphs.
    """

    graph: Graph
    result: "Series[Any]"

    def __enter__(self: GraphCreateResult) -> Graph:
        return self.graph

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.graph.drop()
