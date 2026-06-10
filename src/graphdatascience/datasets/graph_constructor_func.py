from typing import Protocol

from pandas import DataFrame

from graphdatascience.graph import Graph


class GraphConstructorFunc(Protocol):
    def __call__(
        self,
        graph_name: str,
        nodes: DataFrame | list[DataFrame],
        relationships: DataFrame | list[DataFrame] | None = None,
        concurrency: int | None = None,
        undirected_relationship_types: list[str] | None = None,
    ) -> Graph: ...
