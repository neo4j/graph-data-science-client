from graphdatascience.graph.graph_api import Graph
from graphdatascience.model.model_catalog_protocol import ModelCatalogProtocol
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.node_embedding.graphsage_endpoints import GraphSageEndpoints
from graphdatascience.procedure_surface.api.node_embedding.graphsage_supervised_endpoints import (
    GraphSageSupervisedEndpoints,
)
from graphdatascience.procedure_surface.api.node_embedding.graphsage_train_endpoints import (
    GraphSageTrainEndpoints,
)
from graphdatascience.procedure_surface.api.node_embedding.graphsage_unsupervised_endpoints import (
    GraphSageUnsupervisedEndpoints,
)
from graphdatascience.procedure_surface.arrow.node_embedding.graphsage_predict_arrow_endpoints import (
    GraphSagePredictArrowEndpoints,
)


class GraphSageSessionEndpoints(GraphSageEndpoints):
    """
    API for the GraphSage algorithm in GDS Sessions, combining classic training and prediction
    with the unsupervised GraphSage endpoints (the successor of the classic `gds.graph_sage.train`)
    and the supervised (node classification) GraphSage endpoints.
    """

    def __init__(
        self,
        train_endpoints: GraphSageTrainEndpoints,
        predict_endpoints: GraphSagePredictArrowEndpoints,
        catalog_endpoints: ModelCatalogProtocol,
        unsupervised_endpoints: GraphSageUnsupervisedEndpoints,
        supervised_endpoints: GraphSageSupervisedEndpoints,
    ) -> None:
        super().__init__(train_endpoints, predict_endpoints, catalog_endpoints)
        self._arrow_predict_endpoints = predict_endpoints
        self._unsupervised_endpoints = unsupervised_endpoints
        self._supervised_endpoints = supervised_endpoints

    def compute(
        self,
        G: Graph,
        model_name: str,
        *,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
        batch_size: int = 100,
    ) -> JobHandle:
        """Start a classic GraphSage prediction and return a
        :class:`~graphdatascience.procedure_surface.api.job_handle.JobHandle`.

        The handle exposes ``stream`` / ``mutate`` / ``write`` so the caller can decide how to
        materialize the prediction after the computation is started.
        """
        return self._arrow_predict_endpoints.compute(
            G,
            model_name,
            relationship_types=relationship_types,
            node_labels=node_labels,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
            batch_size=batch_size,
        )

    @property
    def unsupervised(self) -> GraphSageUnsupervisedEndpoints:
        """
        Endpoints for the unsupervised GraphSage.

        Returns
        -------
        GraphSageUnsupervisedEndpoints
        """
        return self._unsupervised_endpoints

    @property
    def supervised(self) -> GraphSageSupervisedEndpoints:
        """
        Endpoints for the supervised (node classification) GraphSage.

        Returns
        -------
        GraphSageSupervisedEndpoints
        """
        return self._supervised_endpoints
