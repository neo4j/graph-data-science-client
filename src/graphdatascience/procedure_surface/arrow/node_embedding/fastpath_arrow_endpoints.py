from contextlib import contextmanager
from typing import Any, Iterator

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_TYPES
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.node_embedding.fastpath_endpoints import (
    FastPathEndpoints,
    FastPathMutateResult,
    FastPathWriteResult,
)
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol

FAST_PATH_ENDPOINT = "v2/embeddings.fastPath"


class FeatureNotEnabledError(Exception):
    """Raised when endpoint is invoked against a session that does not have the feature enabled."""


_NOT_ENABLED_MESSAGE = (
    "{feature} is not enabled for this session. "
    "Please reach out to the Neo4j GDS team to have it enabled for your session."
)


@contextmanager
def _translate_feature_not_enabled(endpoint: str, feature: str) -> Iterator[None]:
    """Translate the session's "unsupported action" error into a clear feature-not-enabled error."""
    try:
        yield
    except Exception as e:
        message = str(e)
        if "Unsupported action" in message and endpoint in message:
            raise FeatureNotEnabledError(_NOT_ENABLED_MESSAGE.format(feature=feature)) from e
        raise


class FastPathArrowEndpoints(FastPathEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None = None,
        show_progress: bool = True,
    ):
        self._node_property_endpoints = NodePropertyEndpointsHelper(
            arrow_client, write_protocol, show_progress=show_progress
        )

    def compute(
        self,
        G: Graph,
        base_node_label: str,
        event_node_label: str,
        embedding_dimension: int,
        lookback_horizon: int,
        num_time_anchors: int,
        *,
        event_node_categorical_properties: list[str] = [],
        relationship_types: list[str] = ALL_TYPES,
        context_node_label: str | None = None,
        decay_rate: float = 1.0,
        event_node_feature_vector_property: str | None = None,
        first_relationship_type: str | None = None,
        event_node_ignored_category: int = -1,
        next_relationship_type: str | None = None,
        observation_time: float | None = None,
        base_node_observation_time_property: str | None = None,
        random_seed: Any | None = None,
        smoothing_rate: float = 0.0,
        smoothing_window: int = 0,
        event_node_time_property: str | None = None,
        job_id: str | None = None,
    ) -> JobHandle:
        """Start the FastPath algorithm and return a :class:`JobHandle`.

        The handle exposes ``mutate`` / ``write`` / ``stream`` so the caller can
        decide how to materialize the result after the computation is started.
        Mirrors :meth:`mutate` but omits the property name, which is supplied to
        the handle instead.
        """
        config = self._node_property_endpoints.create_base_config(
            G,
            base_node_label=base_node_label,
            event_node_categorical_properties=event_node_categorical_properties,
            context_node_label=context_node_label,
            decay_rate=decay_rate,
            embedding_dimension=embedding_dimension,
            event_node_feature_vector_property=event_node_feature_vector_property,
            event_node_label=event_node_label,
            first_relationship_type=first_relationship_type,
            event_node_ignored_category=event_node_ignored_category,
            lookback_horizon=lookback_horizon,
            next_relationship_type=next_relationship_type,
            num_time_anchors=num_time_anchors,
            observation_time=observation_time,
            base_node_observation_time_property=base_node_observation_time_property,
            random_seed=random_seed,
            relationship_types=relationship_types,
            smoothing_rate=smoothing_rate,
            smoothing_window=smoothing_window,
            event_node_time_property=event_node_time_property,
            job_id=job_id,
        )

        with _translate_feature_not_enabled(FAST_PATH_ENDPOINT, "FastPath"):
            return self._node_property_endpoints.run_job(G, FAST_PATH_ENDPOINT, config)

    def mutate(
        self,
        G: Graph,
        base_node_label: str,
        event_node_label: str,
        mutate_property: str,
        embedding_dimension: int,
        lookback_horizon: int,
        num_time_anchors: int,
        *,
        event_node_categorical_properties: list[str] = [],
        relationship_types: list[str] = ALL_TYPES,
        context_node_label: str | None = None,
        decay_rate: float = 1.0,
        event_node_feature_vector_property: str | None = None,
        first_relationship_type: str | None = None,
        event_node_ignored_category: int = -1,
        next_relationship_type: str | None = None,
        observation_time: float | None = None,
        base_node_observation_time_property: str | None = None,
        random_seed: Any | None = None,
        smoothing_rate: float = 0.0,
        smoothing_window: int = 0,
        event_node_time_property: str | None = None,
        job_id: str | None = None,
    ) -> FastPathMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            base_node_label=base_node_label,
            event_node_categorical_properties=event_node_categorical_properties,
            context_node_label=context_node_label,
            decay_rate=decay_rate,
            embedding_dimension=embedding_dimension,
            event_node_feature_vector_property=event_node_feature_vector_property,
            event_node_label=event_node_label,
            first_relationship_type=first_relationship_type,
            event_node_ignored_category=event_node_ignored_category,
            lookback_horizon=lookback_horizon,
            next_relationship_type=next_relationship_type,
            num_time_anchors=num_time_anchors,
            observation_time=observation_time,
            base_node_observation_time_property=base_node_observation_time_property,
            random_seed=random_seed,
            relationship_types=relationship_types,
            smoothing_rate=smoothing_rate,
            smoothing_window=smoothing_window,
            event_node_time_property=event_node_time_property,
            job_id=job_id,
        )

        with _translate_feature_not_enabled(FAST_PATH_ENDPOINT, "FastPath"):
            result = self._node_property_endpoints.run_job_and_mutate(FAST_PATH_ENDPOINT, config, mutate_property)

        return FastPathMutateResult(**result)

    def stream(
        self,
        G: Graph,
        base_node_label: str,
        event_node_label: str,
        embedding_dimension: int,
        lookback_horizon: int,
        num_time_anchors: int,
        *,
        event_node_categorical_properties: list[str] = [],
        relationship_types: list[str] = ALL_TYPES,
        context_node_label: str | None = None,
        decay_rate: float = 1.0,
        event_node_feature_vector_property: str | None = None,
        first_relationship_type: str | None = None,
        event_node_ignored_category: int = -1,
        next_relationship_type: str | None = None,
        observation_time: float | None = None,
        base_node_observation_time_property: str | None = None,
        random_seed: Any | None = None,
        smoothing_rate: float = 0.0,
        smoothing_window: int = 0,
        event_node_time_property: str | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            base_node_label=base_node_label,
            event_node_categorical_properties=event_node_categorical_properties,
            context_node_label=context_node_label,
            decay_rate=decay_rate,
            embedding_dimension=embedding_dimension,
            event_node_feature_vector_property=event_node_feature_vector_property,
            event_node_label=event_node_label,
            first_relationship_type=first_relationship_type,
            event_node_ignored_category=event_node_ignored_category,
            lookback_horizon=lookback_horizon,
            next_relationship_type=next_relationship_type,
            num_time_anchors=num_time_anchors,
            observation_time=observation_time,
            base_node_observation_time_property=base_node_observation_time_property,
            random_seed=random_seed,
            relationship_types=relationship_types,
            smoothing_rate=smoothing_rate,
            smoothing_window=smoothing_window,
            event_node_time_property=event_node_time_property,
            job_id=job_id,
        )

        with _translate_feature_not_enabled(FAST_PATH_ENDPOINT, "FastPath"):
            return self._node_property_endpoints.run_job_and_stream(FAST_PATH_ENDPOINT, G, config)

    def write(
        self,
        G: Graph,
        base_node_label: str,
        event_node_label: str,
        write_property: str,
        embedding_dimension: int,
        lookback_horizon: int,
        num_time_anchors: int,
        *,
        event_node_categorical_properties: list[str] = [],
        relationship_types: list[str] = ALL_TYPES,
        context_node_label: str | None = None,
        decay_rate: float = 1.0,
        event_node_feature_vector_property: str | None = None,
        first_relationship_type: str | None = None,
        event_node_ignored_category: int = -1,
        next_relationship_type: str | None = None,
        observation_time: float | None = None,
        base_node_observation_time_property: str | None = None,
        random_seed: Any | None = None,
        smoothing_rate: float = 0.0,
        smoothing_window: int = 0,
        event_node_time_property: str | None = None,
        write_concurrency: int | None = None,
        job_id: str | None = None,
    ) -> FastPathWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            base_node_label=base_node_label,
            event_node_categorical_properties=event_node_categorical_properties,
            context_node_label=context_node_label,
            decay_rate=decay_rate,
            embedding_dimension=embedding_dimension,
            event_node_feature_vector_property=event_node_feature_vector_property,
            event_node_label=event_node_label,
            first_relationship_type=first_relationship_type,
            event_node_ignored_category=event_node_ignored_category,
            lookback_horizon=lookback_horizon,
            next_relationship_type=next_relationship_type,
            num_time_anchors=num_time_anchors,
            observation_time=observation_time,
            base_node_observation_time_property=base_node_observation_time_property,
            random_seed=random_seed,
            relationship_types=relationship_types,
            smoothing_rate=smoothing_rate,
            smoothing_window=smoothing_window,
            event_node_time_property=event_node_time_property,
            job_id=job_id,
        )

        with _translate_feature_not_enabled(FAST_PATH_ENDPOINT, "FastPath"):
            result = self._node_property_endpoints.run_job_and_write(
                FAST_PATH_ENDPOINT,
                G,
                config,
                property_overwrites=write_property,
                write_concurrency=write_concurrency,
            )

        return FastPathWriteResult(**result)
