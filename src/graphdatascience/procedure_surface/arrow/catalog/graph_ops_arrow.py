from __future__ import annotations

import logging
from typing import List

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize
from graphdatascience.graph.graph_info import GraphInfo, GraphInfoWithDegrees
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.session.aura_api_responses import GraphMapping
from graphdatascience.session.graph_mapping_context import GraphMappingContext


class GraphOpsArrow:
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        graph_mapping_context: GraphMappingContext | None = None,
    ):
        self._arrow_client = arrow_client
        self._graph_mapping_context = graph_mapping_context

    def list(self, graph_name: str | None = None, include_mappings: bool = True) -> list[GraphInfoWithDegrees]:
        payload = {"graphName": graph_name} if graph_name else {}

        result = self._arrow_client.do_action_with_retry("v2/graph.list", payload)

        graphs = [GraphInfoWithDegrees(**row) for row in deserialize(result)]

        if include_mappings and self._graph_mapping_context:
            graphs.extend(self._foreign_graphs(graph_name, graphs))

        return graphs

    def drop(self, graph_name: str, fail_if_missing: bool | None = None) -> GraphInfo | None:
        config = ConfigConverter.convert_to_gds_config(graph_name=graph_name, fail_if_missing=fail_if_missing)
        result = self._arrow_client.do_action_with_retry("v2/graph.drop", config)
        deserialized_results = deserialize(result)

        if len(deserialized_results) == 1:
            graph_info = GraphInfo(**deserialized_results[0])
            if self._graph_mapping_context:
                self._graph_mapping_context.unregister(graph_name)
            return graph_info
        else:
            return None

    def _foreign_graphs(
        self, graph_name: str | None, session_graphs: List[GraphInfoWithDegrees]
    ) -> List[GraphInfoWithDegrees]:
        # Graphs registered as mappings but not present in this session were created by
        # another API (e.g. the Cypher API) in a different session. They are listed so
        # both APIs see one catalog, but cannot be used from this session.
        if self._graph_mapping_context is None:
            return []

        try:
            mappings = self._graph_mapping_context.mappings(graph_name)
        except Exception as e:
            logging.getLogger(__name__).warning(f"Failed to list graph mappings: {e}")
            return []

        session_graph_names = {g.graph_name for g in session_graphs}
        return [self._foreign_graph_info(m) for m in mappings if m.graph_name not in session_graph_names]

    @staticmethod
    def _foreign_graph_info(mapping: GraphMapping) -> GraphInfoWithDegrees:
        return GraphInfoWithDegrees(
            graph_name=mapping.graph_name,
            database="",
            database_location=f"session:{mapping.session_id}",
            configuration={},
            memory_usage=None,
            size_in_bytes=0,
            node_count=0,
            relationship_count=0,
            creation_time=mapping.created_at,
            modification_time=mapping.created_at,
            schemaWithOrientation={},
            density=0.0,
            degree_distribution={},
        )
