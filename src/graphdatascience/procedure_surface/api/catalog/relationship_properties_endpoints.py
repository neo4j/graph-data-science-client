from __future__ import annotations

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.catalog.relationships_data_frame import RelationshipsDataFrame
from graphdatascience.procedure_surface.api.catalog.relationships_endpoints import (
    RelationshipsEndpoints,
    RelationshipsWriteResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_TYPES


class RelationshipPropertiesEndpoints:
    """Endpoints for streaming several relationship properties"""

    def __init__(self, relationships_endpoints: RelationshipsEndpoints) -> None:
        self._relationships_endpoints = relationships_endpoints

    def stream(
        self,
        G: Graph,
        relationship_properties: list[str],
        relationship_types: list[str] = ALL_TYPES,
        *,
        concurrency: int | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
    ) -> RelationshipsDataFrame:
        """
        Streams the specified relationship properties for all relationships of the specified types.

        Parameters
        ----------
        G
           Graph object to use
        relationship_properties : list[str]
            The relationship properties to stream
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
            The streamed relationships [sourceNodeId, targetNodeId, relationshipType], followed by one column per
            streamed relationship property. Offers a ``by_rel_type()`` method to reshape the relationships by
            relationship type.
        """
        return self._relationships_endpoints.stream(
            G,
            relationship_types,
            relationship_properties,
            concurrency=concurrency,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
        )

    def write(
        self,
        G: Graph,
        relationship_type: str,
        relationship_properties: list[str],
        *,
        concurrency: int | None = None,
        write_concurrency: int | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        job_id: str | None = None,
    ) -> RelationshipsWriteResult:
        """
        Writes the specified relationship properties of the given relationship type from the graph to the database.

        Parameters
        ----------
        G
           Graph object to use
        relationship_type : str
            The relationship type to write to the database
        relationship_properties : list[str]
            The relationship properties to write
        concurrency
            Number of concurrent threads to use.
        write_concurrency
            Number of concurrent threads to use for writing.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        job_id
            Identifier for the computation.
        Returns
        -------
        RelationshipsWriteResult
            Execution metrics and statistics
        """
        return self._relationships_endpoints.write(
            G,
            relationship_type,
            relationship_properties,
            concurrency=concurrency,
            write_concurrency=write_concurrency,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            job_id=job_id,
        )
