from types import TracebackType
from typing import Any, NamedTuple, Optional, Type, TypeVar

from pandas import Series

from .graph_object import Graph

TGraphCreateResult = TypeVar("TGraphCreateResult", bound="GraphCreateResult")


class GraphCreateResult(NamedTuple):
    """
    A result object returned by endpoints which create graphs.
    """

    graph: Graph
    result: "Series[Any]"

    def __enter__(self: TGraphCreateResult) -> Graph:
        return self.graph

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.graph.drop()
