from typing import Any
from unittest import mock

from pandas import DataFrame

from graphdatascience.datasets.graph_constructor_func import GraphConstructorFunc
from graphdatascience.graph import Graph
from graphdatascience.graph.graph_backend import GraphBackend


class CollectingGraphConstructor(GraphConstructorFunc):
    def __init__(self) -> None:
        self.calls: dict[str, Any] = {}

    def __call__(
        self,
        graph_name: str,
        nodes: DataFrame | list[DataFrame],
        relationships: DataFrame | list[DataFrame] | None = None,
        concurrency: int | None = None,
        undirected_relationship_types: list[str] | None = None,
    ) -> Graph:
        self.calls[graph_name] = {
            "nodes": nodes,
            "relationships": relationships,
            "concurrency": concurrency,
            "undirected_relationship_types": undirected_relationship_types,
        }

        return Graph(graph_name, mock.Mock(speck=GraphBackend))
