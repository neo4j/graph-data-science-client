import networkx as nx
import pandas as pd

from graphdatascience.datasets.nx_loader import NXLoader as NXLoaderV2

from ..error.client_only_endpoint import client_only_endpoint
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..server_version.compatible_with import compatible_with
from ..server_version.server_version import ServerVersion
from .graph_object import Graph
from .v2 import GraphV2
from .v2.graph_backend_cypher import get_graph


class NXLoader(UncallableNamespace, IllegalAttrChecker):
    def _construct_graph(
        self,
        graph_name: str,
        nodes: pd.DataFrame | list[pd.DataFrame],
        relationships: pd.DataFrame | list[pd.DataFrame] | None = None,
        concurrency: int | None = None,
        undirected_relationship_types: list[str] | None = None,
    ) -> GraphV2:
        if concurrency is None:
            concurrency = 4

        constructor = self._query_runner.create_graph_constructor(
            graph_name, concurrency, undirected_relationship_types
        )

        if isinstance(nodes, pd.DataFrame):
            nodes = [nodes]
        if isinstance(relationships, pd.DataFrame):
            relationships = [relationships]
        if relationships is None:
            relationships = []

        constructor.run(nodes, relationships)
        return get_graph(graph_name, self._query_runner)

    @client_only_endpoint("gds.graph.networkx")
    @compatible_with("load", min_inclusive=ServerVersion(2, 1, 0))
    def load(self, nx_G: nx.Graph, graph_name: str, concurrency: int = 4) -> Graph:
        G_V2 = NXLoaderV2(self._construct_graph).load(nx_G, graph_name, concurrency)
        return Graph(G_V2.name(), self._query_runner)
