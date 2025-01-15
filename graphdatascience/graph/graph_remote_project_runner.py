from __future__ import annotations

from typing import Optional

from ..call_parameters import CallParameters
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..query_runner.session_query_runner import SessionQueryRunner
from ..server_version.compatible_with import compatible_with
from ..server_version.server_version import ServerVersion
from .graph_create_result import GraphCreateResult
from .graph_object import Graph


class GraphProjectRemoteRunner(IllegalAttrChecker):
    @compatible_with("project", min_inclusive=ServerVersion(2, 7, 0))
    def __call__(
        self,
        graph_name: str,
        query: str,
        job_id: Optional[str] = None,
        concurrency: int = 4,
        undirected_relationship_types: Optional[list[str]] = None,
        inverse_indexed_relationship_types: Optional[list[str]] = None,
        batch_size: Optional[int] = None,
        logging: bool = True,
    ) -> GraphCreateResult:
        if inverse_indexed_relationship_types is None:
            inverse_indexed_relationship_types = []
        if undirected_relationship_types is None:
            undirected_relationship_types = []

        arrow_configuration = {}
        if batch_size is not None:
            arrow_configuration["batchSize"] = batch_size

        params = CallParameters(
            graph_name=graph_name,
            query=query,
            job_id=job_id,
            concurrency=concurrency,
            undirected_relationship_types=undirected_relationship_types,
            inverse_indexed_relationship_types=inverse_indexed_relationship_types,
            arrow_configuration=arrow_configuration,
        )

        result = self._query_runner.call_procedure(
            endpoint=SessionQueryRunner.GDS_REMOTE_PROJECTION_PROC_NAME,
            params=params,
            logging=True,
        ).squeeze()
        return GraphCreateResult(Graph(graph_name, self._query_runner), result)
