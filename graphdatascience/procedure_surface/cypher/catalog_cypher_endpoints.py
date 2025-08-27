from __future__ import annotations

from typing import List, Optional, Union

from ...call_parameters import CallParameters
from ...graph.graph_object import Graph
from ...query_runner.query_runner import QueryRunner
from ..api.catalog_endpoints import CatalogEndpoints, GraphFilterResult, GraphListResult
from ..api.graph_sampling_endpoints import GraphSamplingEndpoints
from ..utils.config_converter import ConfigConverter
from .graph_sampling_cypher_endpoints import GraphSamplingCypherEndpoints


class CatalogCypherEndpoints(CatalogEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def list(self, G: Optional[Union[Graph, str]] = None) -> List[GraphListResult]:
        graph_name = G if isinstance(G, str) else G.name() if G is not None else None
        params = CallParameters(graphName=graph_name) if graph_name else CallParameters()

        result = self._query_runner.call_procedure(endpoint="gds.graph.list", params=params)
        return [GraphListResult(**row.to_dict()) for _, row in result.iterrows()]

    def drop(self, G: Union[Graph, str], fail_if_missing: Optional[bool] = None) -> Optional[GraphListResult]:
        graph_name = G if isinstance(G, str) else G.name()

        params = (
            CallParameters(graphName=graph_name, failIfMissing=fail_if_missing)
            if fail_if_missing is not None
            else CallParameters(graphName=graph_name)
        )

        result = self._query_runner.call_procedure(endpoint="gds.graph.drop", params=params)
        if len(result) > 0:
            return GraphListResult(**result.iloc[0].to_dict())
        else:
            return None

    def filter(
        self,
        G: Graph,
        graph_name: str,
        node_filter: str,
        relationship_filter: str,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
    ) -> GraphFilterResult:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            jobId=job_id,
        )

        params = CallParameters(
            graph_name=graph_name,
            from_graph_name=G.name(),
            node_filter=node_filter,
            relationship_filter=relationship_filter,
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.graph.filter", params=params).squeeze()
        return GraphFilterResult(**result.to_dict())

    @property
    def sample(self) -> GraphSamplingEndpoints:
        """Get graph sampling endpoints.

        Returns:
            GraphSamplingEndpoints: Graph sampling endpoints for cypher implementation.
        """
        return GraphSamplingCypherEndpoints(self._query_runner)
