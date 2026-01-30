from typing import Any
from unittest import mock

from pandas import DataFrame

from graphdatascience.datasets.graph_constructor_func import GraphConstructorFunc
from graphdatascience.graph.v2 import GraphBackend, GraphV2


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
    ) -> GraphV2:
        self.calls[graph_name] = {
            "nodes": nodes,
            "relationships": relationships,
            "concurrency": concurrency,
            "undirected_relationship_types": undirected_relationship_types,
        }

        return GraphV2(graph_name, mock.Mock(speck=GraphBackend))
