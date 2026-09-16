import warnings
from typing import Any

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.node_embedding.config import (
    FastRPConfig,
    GBClassifierConfig,
    GraphSAGEConfig,
    IdentityConfig,
    MLPClassifierConfig,
)
from graphdatascience.procedure_surface.api.node_embedding.embedding_endpoints import (
    EmbeddingCreateResult,
    EmbeddingEndpoints,
    EmbeddingTrainConfig,
    EmbeddingTrainResult,
    EncodeConfig,
)
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol


class EmbeddingArrowEndpoints(EmbeddingEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None = None,
        show_progress: bool = True,
    ) -> None:
        self._arrow_client = arrow_client
        self._endpoints_helper = NodePropertyEndpointsHelper(arrow_client, write_protocol, show_progress)
        warnings.warn(
            "embeddings.encode is a preview feature and may change or be removed in future releases.",
            UserWarning,
            stacklevel=2,
        )

    def create(
        self,
        G: Graph,
        *,
        graph_encoder: str | (FastRPConfig | IdentityConfig),
        random_seed: int | None = None,
        mutate_property: str,
        job_id: str | None = None,
        node_labels: list[str] = ["*"],
        relationship_types: list[str] = ["*"],
        feature_properties: list[str] = [],
    ) -> EmbeddingCreateResult:
        extra_kwargs: dict[str, Any] = {}
        if random_seed is not None:
            extra_kwargs["random_seed"] = random_seed
        config = EncodeConfig(graph_encoder=graph_encoder, **extra_kwargs).model_dump(exclude={"task_name"})
        config.update(
            job_id=job_id,
            node_labels=node_labels,
            relationship_types=relationship_types,
            feature_properties=feature_properties,
        )
        config = self._endpoints_helper.create_base_config(G=G, **config)
        result = self._endpoints_helper.run_job_and_mutate("v2/embeddings.encode", config, mutate_property)
        return EmbeddingCreateResult(**result)

    def train(
        self,
        G: Graph,
        *,
        graph_encoder: FastRPConfig | GraphSAGEConfig | IdentityConfig,
        decoder: GBClassifierConfig | MLPClassifierConfig,
        model_save_name: str,
        target_label: str,
        target_property: str,
        num_epochs: int | None = None,
        batch_size: int | None = None,
        num_trials: int = 1,
        random_seed: int | None = None,
        job_id: str | None = None,
        node_labels: list[str] = ["*"],
        relationship_types: list[str] = ["*"],
        feature_properties: list[str] = [],
    ) -> EmbeddingTrainResult:
        extra_kwargs: dict[str, Any] = {}
        if random_seed is not None:
            extra_kwargs["random_seed"] = random_seed
        config = EmbeddingTrainConfig(
            graph_encoder=graph_encoder,
            decoder=decoder,
            model_save_name=model_save_name,
            target_label=target_label,
            target_property=target_property,
            num_epochs=num_epochs,
            batch_size=batch_size,
            num_trials=num_trials,
            **extra_kwargs,
        ).model_dump(exclude={"task_name"})
        config.update(
            job_id=job_id,
            node_labels=node_labels,
            relationship_types=relationship_types,
            feature_properties=feature_properties,
        )
        config = self._endpoints_helper.create_base_config(G=G, **config)
        result = self._endpoints_helper.run_job_and_get_summary("v2/embeddings.train", config)
        return EmbeddingTrainResult(**result)
