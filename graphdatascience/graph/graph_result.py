from dataclasses import dataclass
from types import TracebackType
from typing import Optional, Type, TypeVar

from ..graph.graph_object import Graph

TGraphResult = TypeVar("TGraphResult", bound="GraphResult")


@dataclass(frozen=True)
class GraphResult:
    graph: Graph
    result: "Series[Any]"

    def __iter__(self):
        return iter((self.graph, self.result))

    def __enter__(self: TGraphResult) -> Graph:
        return self.graph

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.graph.drop()

    def __getitem__(self, item):
        if item == 0:
            return self.graph
        if item == 1:
            return self.result

        raise KeyError("Key must be between 0 and 1")
