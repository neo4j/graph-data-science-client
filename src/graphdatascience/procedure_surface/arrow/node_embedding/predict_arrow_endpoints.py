import warnings
from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.node_embedding.predict_endpoints import (
    PredictConfig,
    PredictEndpoints,
    PredictMutateResult,
    PredictStatsResult,
    PredictWriteResult,
)
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol


class PredictArrowEndpoints(PredictEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None = None,
        show_progress: bool = True,
    ) -> None:
        self._arrow_client = arrow_client
        self._endpoints_helper = NodePropertyEndpointsHelper(arrow_client, write_protocol, show_progress)
        warnings.warn(
            "embeddings.predict is a preview feature and may change or be removed in future releases.",
            UserWarning,
            stacklevel=2,
        )

    def compute(
        self,
        G: Graph,
        *,
        model_name: str,
        random_seed: int | None = None,
        job_id: str | None = None,
        node_labels: list[str] = ["*"],
        relationship_types: list[str] = ["*"],
        feature_properties: list[str] = [],
    ) -> JobHandle:
        config = self._build_config(
            G,
            model_name=model_name,
            random_seed=random_seed,
            job_id=job_id,
            node_labels=node_labels,
            relationship_types=relationship_types,
            feature_properties=feature_properties,
        )
        return self._endpoints_helper.run_job(G, "v2/embeddings.predict", config)

    def stream(
        self,
        G: Graph,
        *,
        model_name: str,
        random_seed: int | None = None,
        job_id: str | None = None,
        node_labels: list[str] = ["*"],
        relationship_types: list[str] = ["*"],
        feature_properties: list[str] = [],
    ) -> DataFrame:
        config = self._build_config(
            G,
            model_name=model_name,
            random_seed=random_seed,
            job_id=job_id,
            node_labels=node_labels,
            relationship_types=relationship_types,
            feature_properties=feature_properties,
        )
        return self._endpoints_helper.run_job_and_stream("v2/embeddings.predict", G, config)

    def mutate(self, G: Graph, *, model_name: str, random_seed: int | None = None, mutate_property: str,
               job_id: str | None = None, node_labels: list[str] = ["*"], relationship_types: list[str] = ["*"],
               feature_properties: list[str] = []) -> PredictMutateResult:
        config = self._build_config(
            G,
            model_name=model_name,
            random_seed=random_seed,
            job_id=job_id,
            node_labels=node_labels,
            relationship_types=relationship_types,
            feature_properties=feature_properties,
        )
        result = self._endpoints_helper.run_job_and_mutate("v2/embeddings.predict", config, mutate_property)
        return PredictMutateResult(**result)

    def stats(self, G: Graph, *, model_name: str, random_seed: int | None = None, job_id: str | None = None,
              node_labels: list[str] = ["*"], relationship_types: list[str] = ["*"],
              feature_properties: list[str] = []) -> PredictStatsResult:
        config = self._build_config(
            G,
            model_name=model_name,
            random_seed=random_seed,
            job_id=job_id,
            node_labels=node_labels,
            relationship_types=relationship_types,
            feature_properties=feature_properties,
        )
        result = self._endpoints_helper.run_job_and_get_summary("v2/embeddings.predict", config)
        return PredictStatsResult(**result)

    def write(
        self,
        G: Graph,
        *,
        model_name: str,
        random_seed: int | None = None,
        write_property: str,
        write_concurrency: int | None = None,
        job_id: str | None = None,
        node_labels: list[str] = ["*"],
        relationship_types: list[str] = ["*"],
        feature_properties: list[str] = [],
    ) -> PredictWriteResult:
        config = self._build_config(
            G,
            model_name=model_name,
            random_seed=random_seed,
            job_id=job_id,
            node_labels=node_labels,
            relationship_types=relationship_types,
            feature_properties=feature_properties,
        )
        result = self._endpoints_helper.run_job_and_write(
            "v2/embeddings.predict", G, config, write_property, write_concurrency
        )
        return PredictWriteResult(**result)

    def _build_config(
        self,
        G: Graph,
        *,
        model_name: str,
        random_seed: int | None = None,
        job_id: str | None = None,
        node_labels: list[str] = ["*"],
        relationship_types: list[str] = ["*"],
        feature_properties: list[str] = [],
    ) -> dict[str, Any]:
        extra_kwargs: dict[str, Any] = {}
        if random_seed is not None:
            extra_kwargs["random_seed"] = random_seed
        config = PredictConfig(model_name=model_name, **extra_kwargs).model_dump(exclude={"task_name"})
        config.update(
            job_id=job_id,
            node_labels=node_labels,
            relationship_types=relationship_types,
            feature_properties=feature_properties,
        )
        return self._endpoints_helper.create_base_config(G=G, **config)
