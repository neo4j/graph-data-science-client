import logging
from abc import ABC

import pandas as pd

from graphdatascience.datasets.ogb_loader import OGBLLoader as OGBLLoaderV2
from graphdatascience.datasets.ogb_loader import OGBNLoader as OGBNLoaderV2

from ..error.client_only_endpoint import client_only_endpoint
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..graph.graph_object import Graph
from ..query_runner.query_runner import QueryRunner
from ..server_version.compatible_with import compatible_with
from ..server_version.server_version import ServerVersion
from .v2 import GraphV2
from .v2.graph_backend_cypher import get_graph


class OGBLoader(UncallableNamespace, IllegalAttrChecker, ABC):
    def __init__(self, query_runner: QueryRunner, namespace: str, server_version: ServerVersion):
        super().__init__(query_runner, namespace, server_version)
        self._logger = logging.getLogger()

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


class OGBNLoader(OGBLoader):
    @client_only_endpoint("gds.graph.ogbn")
    @compatible_with("load", min_inclusive=ServerVersion(2, 1, 0))
    def load(
        self,
        dataset_name: str,
        dataset_root_path: str = "dataset",
        graph_name: str | None = None,
        concurrency: int = 4,
    ) -> Graph:
        G_V2 = OGBNLoaderV2(self._construct_graph).load(dataset_name, dataset_root_path, graph_name, concurrency)
        return Graph(G_V2.name(), query_runner=self._query_runner)


class OGBLLoader(OGBLoader):
    @client_only_endpoint("gds.graph.ogbl")
    @compatible_with("load", min_inclusive=ServerVersion(2, 1, 0))
    def load(
        self,
        dataset_name: str,
        dataset_root_path: str = "dataset",
        graph_name: str | None = None,
        concurrency: int = 4,
    ) -> Graph:
        G_V2 = OGBLLoaderV2(self._construct_graph).load(dataset_name, dataset_root_path, graph_name, concurrency)
        return Graph(G_V2.name(), query_runner=self._query_runner)
