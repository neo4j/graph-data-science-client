from __future__ import annotations

import logging
from dataclasses import dataclass

from graphdatascience.session.aura_api import AuraApi
from graphdatascience.session.aura_api_responses import GraphMapping
from graphdatascience.session.graph_mappings_api import GraphMappings


@dataclass
class GraphMappingContext:
    """
    The identity under which graphs of a GDS session are registered as graph mappings,
    and the API to manage them. Present only for sessions attached to an AuraDB with
    resolvable identity; all interoperability hooks are no-ops when it is absent.
    """

    session_id: str
    database_username: str
    database_uuid: str
    graph_mappings: GraphMappings

    @classmethod
    def from_aura_api(
        cls,
        aura_api: AuraApi,
        session_id: str,
        database_username: str,
        database_uuid: str,
    ) -> GraphMappingContext:
        return cls(session_id, database_username, database_uuid, GraphMappings(aura_api))

    def register(self, graph_name: str) -> None:
        # Best effort: a failed registration must not fail the graph operation that
        # created the graph. The graph remains fully usable within this session.
        try:
            self.graph_mappings.create(self.session_id, graph_name, self.database_username, self.database_uuid)
        except Exception as e:
            logging.getLogger(__name__).warning(
                f"Failed to register the graph mapping for graph `{graph_name}` (session `{self.session_id}`): {e}"
            )

    def unregister(self, graph_name: str) -> None:
        # Best effort: a failed unregistering only leaves an orphaned mapping behind,
        # which is cleaned up together with its session.
        try:
            self.graph_mappings.delete(graph_name, self.database_username, self.database_uuid)
        except Exception as e:
            logging.getLogger(__name__).warning(
                f"Failed to delete the graph mapping for graph `{graph_name}` (session `{self.session_id}`): {e}"
            )

    def mappings(self, graph_name: str | None = None) -> list[GraphMapping]:
        return self.graph_mappings.list(self.database_username, self.database_uuid, graph_name)
