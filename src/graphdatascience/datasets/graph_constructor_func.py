from typing import Protocol

from pandas import DataFrame

from graphdatascience.graph.v2 import GraphV2


class GraphConstructorFunc(Protocol):
    def __call__(
        self,
        graph_name: str,
        nodes: DataFrame | list[DataFrame],
        relationships: DataFrame | list[DataFrame] | None = None,
        concurrency: int | None = None,
        undirected_relationship_types: list[str] | None = None,
    ) -> GraphV2: ...
