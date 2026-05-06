from __future__ import annotations

from abc import ABC, abstractmethod

from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.relationships_endpoints import CollapsePathResult


class CollapsePathEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        path_templates: list[list[str]],
        mutate_relationship_type: str,
        *,
        allow_self_loops: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
    ) -> CollapsePathResult:
        """
        Collapse each existing path in the graph into a single relationship.

        Parameters
        ----------
        G
           Graph object to use
        path_templates : list[list[str]]
            A path template is an ordered list of relationship types used for the traversal. The same relationship type can be added multiple times, in order to traverse them as indicated. And, you may specify several path templates to process in one go.
        mutate_relationship_type : str
            Name of the relationship type to store the results in.
        allow_self_loops : bool, default=False
            Whether nodes in the graph can have relationships where start and end nodes are the same.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.

        Returns
        -------
        CollapsePathResult
            Meta data about the generated relationships.
        """
        pass
