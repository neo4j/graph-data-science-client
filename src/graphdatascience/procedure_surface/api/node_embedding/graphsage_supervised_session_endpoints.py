from __future__ import annotations

from abc import abstractmethod

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.node_embedding.graphsage_supervised_endpoints import (
    GraphSageSupervisedEndpoints,
)


class GraphSageSupervisedSessionEndpoints(GraphSageSupervisedEndpoints):
    """Supervised (node classification) GraphSage endpoints as exposed by a GDS Session."""

    @abstractmethod
    def compute(
        self,
        G: Graph,
        model_name: str,
        feature_properties: list[str],
        *,
        batch_size: int | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        job_id: str | None = None,
    ) -> JobHandle:
        """
        Starts a supervised GraphSage prediction as a job and returns a handle to it.

        The computation runs without materializing its result. Use the returned handle to
        mutate, write or stream the predicted classes and probabilities afterwards.

        Parameters
        ----------
        G
           Graph object to use
        model_name
            Name of the trained model to use for prediction.
        feature_properties
            Names of the node properties to use as input features.
        batch_size
            Number of nodes to process in each batch.
        random_seed
            Seed for reproducible results.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        job_id
            Identifier for the computation.

        Returns
        -------
        JobHandle
            Handle to the started computation
        """
        pass
