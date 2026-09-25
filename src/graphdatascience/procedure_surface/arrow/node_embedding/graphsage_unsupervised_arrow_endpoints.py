from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.node_embedding.graphsage_results import (
    GraphSageUnsupervisedMutateResult,
    GraphSageUnsupervisedTrainResult,
    GraphSageUnsupervisedWriteResult,
)
from graphdatascience.procedure_surface.api.node_embedding.graphsage_unsupervised_endpoints import (
    GraphSageUnsupervisedEndpoints,
)
from graphdatascience.procedure_surface.api.node_embedding.graphsage_unsupervised_model import (
    GraphSageUnsupervisedModel,
)
from graphdatascience.procedure_surface.arrow.model.model_catalog_arrow_endpoints import ModelCatalogArrowEndpoints
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol


class GraphSageUnsupervisedArrowEndpoints(GraphSageUnsupervisedEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None,
        show_progress: bool = True,
    ):
        self._arrow_client = arrow_client
        self._write_protocol = write_protocol
        self._show_progress = show_progress
        self._node_property_endpoints = NodePropertyEndpointsHelper(
            arrow_client, write_protocol=write_protocol, show_progress=show_progress
        )
        self._model_catalog = ModelCatalogArrowEndpoints(arrow_client)

    def train(
        self,
        G: Graph,
        model_name: str,
        feature_properties: list[str],
        *,
        epochs: int,
        activation_function: str = "relu",
        aggregator: str = "mean",
        batch_size: int | None = None,
        dropout: float = 0.1,
        embedding_dimension: int = 256,
        layer_normalization: bool = True,
        learning_rate: float = 0.001,
        negative_sampling_ratio: float = 1.0,
        num_neighbors: list[int] = [20, 10],
        num_walks: int = 10,
        walk_depth: int = 3,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        job_id: str | None = None,
    ) -> tuple[GraphSageUnsupervisedModel, GraphSageUnsupervisedTrainResult]:
        config = self._node_property_endpoints.create_base_config(
            G,
            model_name=model_name,
            feature_properties=feature_properties,
            epochs=epochs,
            activation_function=activation_function,
            aggregator=aggregator,
            batch_size=batch_size,
            dropout=dropout,
            embedding_dimension=embedding_dimension,
            layer_normalization=layer_normalization,
            learning_rate=learning_rate,
            negative_sampling_ratio=negative_sampling_ratio,
            num_neighbors=num_neighbors,
            num_walks=num_walks,
            walk_depth=walk_depth,
            random_seed=random_seed,
            relationship_types=relationship_types,
            node_labels=node_labels,
            job_id=job_id,
        )

        result = self._node_property_endpoints.run_job_and_get_summary(
            "v2/embeddings.graphSage.unsupervised.train", config
        )

        model = GraphSageUnsupervisedModel(model_name, self._model_catalog, self)
        train_result = GraphSageUnsupervisedTrainResult(**result)

        return model, train_result

    def stream(
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
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            model_name=model_name,
            feature_properties=feature_properties,
            batch_size=batch_size,
            random_seed=random_seed,
            relationship_types=relationship_types,
            node_labels=node_labels,
            job_id=job_id,
        )
        return self._node_property_endpoints.run_job_and_stream(
            "v2/embeddings.graphSage.unsupervised.predict", G, config
        )

    def write(
        self,
        G: Graph,
        model_name: str,
        feature_properties: list[str],
        write_property: str,
        *,
        batch_size: int | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> GraphSageUnsupervisedWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            model_name=model_name,
            feature_properties=feature_properties,
            batch_size=batch_size,
            random_seed=random_seed,
            relationship_types=relationship_types,
            node_labels=node_labels,
            job_id=job_id,
        )

        raw_result = self._node_property_endpoints.run_job_and_write(
            "v2/embeddings.graphSage.unsupervised.predict",
            G,
            config,
            property_overwrites=write_property,
            write_concurrency=write_concurrency,
        )

        return GraphSageUnsupervisedWriteResult(**raw_result)

    def mutate(
        self,
        G: Graph,
        model_name: str,
        feature_properties: list[str],
        mutate_property: str,
        *,
        batch_size: int | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        job_id: str | None = None,
    ) -> GraphSageUnsupervisedMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            model_name=model_name,
            feature_properties=feature_properties,
            batch_size=batch_size,
            random_seed=random_seed,
            relationship_types=relationship_types,
            node_labels=node_labels,
            job_id=job_id,
        )

        raw_result = self._node_property_endpoints.run_job_and_mutate(
            "v2/embeddings.graphSage.unsupervised.predict",
            config,
            mutate_property,
        )

        return GraphSageUnsupervisedMutateResult(**raw_result)
