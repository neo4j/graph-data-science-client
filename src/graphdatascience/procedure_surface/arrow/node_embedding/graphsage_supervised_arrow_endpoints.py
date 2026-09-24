from collections import OrderedDict

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.node_embedding.graphsage_results import (
    GraphSageRuntimeMutateResult,
    GraphSageRuntimeTrainResult,
    GraphSageRuntimeWriteResult,
)
from graphdatascience.procedure_surface.api.node_embedding.graphsage_supervised_endpoints import (
    GraphSageSupervisedEndpoints,
)
from graphdatascience.procedure_surface.api.node_embedding.graphsage_supervised_model import (
    GraphSageSupervisedModel,
)
from graphdatascience.procedure_surface.arrow.model.model_catalog_arrow_endpoints import ModelCatalogArrowEndpoints
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol


class GraphSageSupervisedArrowEndpoints(GraphSageSupervisedEndpoints):
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
        target_label: str,
        target_property: str,
        epochs: int,
        activation_function: str = "relu",
        aggregator: str = "mean",
        batch_size: int | None = None,
        class_weights: bool = False,
        dropout: float = 0.1,
        embedding_dimension: int = 256,
        epochs_per_val: int = 0,
        layer_normalization: bool = True,
        learning_rate: float = 0.001,
        num_neighbors: list[int] = [20, 10],
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        split_ratios: dict[str, float] = {"TRAIN": 0.6, "TEST": 0.2, "VALID": 0.2},
        job_id: str | None = None,
    ) -> tuple[GraphSageSupervisedModel, GraphSageRuntimeTrainResult]:
        config = self._node_property_endpoints.create_base_config(
            G,
            model_name=model_name,
            feature_properties=feature_properties,
            target_label=target_label,
            target_property=target_property,
            epochs=epochs,
            activation_function=activation_function,
            aggregator=aggregator,
            batch_size=batch_size,
            class_weights=class_weights,
            dropout=dropout,
            embedding_dimension=embedding_dimension,
            epochs_per_val=epochs_per_val,
            layer_normalization=layer_normalization,
            learning_rate=learning_rate,
            num_neighbors=num_neighbors,
            random_seed=random_seed,
            relationship_types=relationship_types,
            node_labels=node_labels,
            split_ratios=split_ratios,
            job_id=job_id,
        )

        result = self._node_property_endpoints.run_job_and_get_summary(
            "v2/embeddings.graphSage.supervised.train", config
        )

        model = GraphSageSupervisedModel(model_name, self._model_catalog, self)
        train_result = GraphSageRuntimeTrainResult(**result)

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
        return self._node_property_endpoints.run_job_and_stream("v2/embeddings.graphSage.supervised.predict", G, config)

    def write(
        self,
        G: Graph,
        model_name: str,
        feature_properties: list[str],
        write_property: str,
        *,
        predicted_probability_property: str | None = None,
        batch_size: int | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> GraphSageRuntimeWriteResult:
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

        # the runtime streams the results under snake_case column names
        property_overwrites: dict[str, str] = {"predicted_class": write_property}
        if predicted_probability_property is not None:
            property_overwrites["predicted_probabilities"] = predicted_probability_property

        raw_result = self._node_property_endpoints.run_job_and_write(
            "v2/embeddings.graphSage.supervised.predict",
            G,
            config,
            property_overwrites=property_overwrites,
            write_concurrency=write_concurrency,
        )

        return GraphSageRuntimeWriteResult(**raw_result)

    def mutate(
        self,
        G: Graph,
        model_name: str,
        feature_properties: list[str],
        mutate_property: str,
        *,
        predicted_probability_property: str | None = None,
        batch_size: int | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        job_id: str | None = None,
    ) -> GraphSageRuntimeMutateResult:
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

        if predicted_probability_property is not None:
            raw_result = self._node_property_endpoints.run_job_and_mutate_multiple(
                "v2/embeddings.graphSage.supervised.predict",
                config,
                OrderedDict(
                    [
                        ("predicted_class", mutate_property),
                        ("predicted_probabilities", predicted_probability_property),
                    ]
                ),
            )
        else:
            raw_result = self._node_property_endpoints.run_job_and_mutate(
                "v2/embeddings.graphSage.supervised.predict",
                config,
                mutate_property,
            )

        return GraphSageRuntimeMutateResult(**raw_result)
