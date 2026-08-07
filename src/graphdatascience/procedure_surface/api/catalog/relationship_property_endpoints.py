from __future__ import annotations

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.catalog.relationships_data_frame import RelationshipsDataFrame
from graphdatascience.procedure_surface.api.catalog.relationships_endpoints import RelationshipsEndpoints
from graphdatascience.procedure_surface.api.default_values import ALL_TYPES


class RelationshipPropertyEndpoints:
    """Endpoints for streaming a single relationship property, delegating to the relationships endpoints."""

    def __init__(self, relationships_endpoints: RelationshipsEndpoints) -> None:
        self._relationships_endpoints = relationships_endpoints

    def stream(
        self,
        G: Graph,
        relationship_property: str,
        relationship_types: list[str] = ALL_TYPES,
        *,
        concurrency: int | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
    ) -> RelationshipsDataFrame:
        """
        Streams the specified relationship property for all relationships of the specified types.

        Parameters
        ----------
        G
           Graph object to use
        relationship_property
            The relationship property to stream
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        concurrency
            Number of concurrent threads to use.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        Returns
        -------
        RelationshipsDataFrame
            The streamed relationships [sourceNodeId, targetNodeId, relationshipType, propertyValue]. Offers a
            ``by_rel_type()`` method to reshape the relationships by relationship type.
        """
        result = self._relationships_endpoints.stream(
            G,
            relationship_types,
            [relationship_property],
            concurrency=concurrency,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
        )

        return RelationshipsDataFrame(result.rename(columns={relationship_property: "propertyValue"}))
