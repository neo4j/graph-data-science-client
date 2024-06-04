from __future__ import annotations

from typing import List, Optional

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..query_runner.aura_db_query_runner import AuraDbQueryRunner
from ..server_version.compatible_with import compatible_with
from .graph_object import Graph
from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.graph_create_result import GraphCreateResult
from graphdatascience.server_version.server_version import ServerVersion


class GraphProjectRemoteRunner(IllegalAttrChecker):
    @compatible_with("project", min_inclusive=ServerVersion(2, 7, 0))
    def __call__(
        self,
        graph_name: str,
        query: str,
        concurrency: int = 4,
        undirected_relationship_types: Optional[List[str]] = None,
        inverse_indexed_relationship_types: Optional[List[str]] = None,
    ) -> GraphCreateResult:
        if inverse_indexed_relationship_types is None:
            inverse_indexed_relationship_types = []
        if undirected_relationship_types is None:
            undirected_relationship_types = []

        params = CallParameters(
            graph_name=graph_name,
            query=query,
            concurrency=concurrency,
            undirected_relationship_types=undirected_relationship_types,
            inverse_indexed_relationship_types=inverse_indexed_relationship_types,
        )

        result = self._query_runner.call_procedure(
            endpoint=AuraDbQueryRunner.GDS_REMOTE_PROJECTION_PROC_NAME,
            params=params,
        ).squeeze()
        return GraphCreateResult(Graph(graph_name, self._query_runner, self._server_version), result)
