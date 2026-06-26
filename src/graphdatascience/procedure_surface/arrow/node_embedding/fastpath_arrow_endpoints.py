import warnings
from contextlib import contextmanager
from typing import Any, Iterator

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_TYPES
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
    "FastPath is not enabled for this session. It is a preview feature; "
    "please reach out to the Neo4j GDS team to have it enabled for your session."
)


@contextmanager
def _translate_feature_not_enabled() -> Iterator[None]:
    """Translate the session's "unsupported action" error into a clear feature-not-enabled error."""
    try:
        yield
    except Exception as e:
        message = str(e)
        if "Unsupported action" in message and FAST_PATH_ENDPOINT in message:
            raise FeatureNotEnabledError(_NOT_ENABLED_MESSAGE) from e
        raise


class FastPathArrowEndpoints(FastPathEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None = None,
        show_progress: bool = True,
    ):
        warnings.warn(
            "FastPath is a preview feature and may change or be removed in future releases.",
            UserWarning,
            stacklevel=2,
        )
        self._node_property_endpoints = NodePropertyEndpointsHelper(
            arrow_client, write_protocol, show_progress=show_progress
        )

    def mutate(
        self,
        G: Graph,
        base_node_label: str,
        event_node_label: str,
        mutate_property: str,
        dimension: int,
        max_elapsed_time: int,
        num_elapsed_times: int,
        *,
        categorical_event_properties: list[str] = [],
        relationship_types: list[str] = ALL_TYPES,
        context_node_label: str | None = None,
        decay_factor: float = 1.0,
        event_features: str | None = None,
        first_relationship_type: str | None = None,
        ignored_event_category: int = -1,
        next_relationship_type: str | None = None,
        output_time: float | None = None,
        output_time_property: str | None = None,
        random_seed: Any | None = None,
        smoothing_rate: float = 0.0,
        smoothing_window: int = 0,
        time_node_property: str | None = None,
    ) -> FastPathMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            base_node_label=base_node_label,
            categorical_event_properties=categorical_event_properties,
            context_node_label=context_node_label,
            decay_factor=decay_factor,
            dimension=dimension,
            event_features=event_features,
            event_node_label=event_node_label,
            first_relationship_type=first_relationship_type,
            ignored_event_category=ignored_event_category,
            max_elapsed_time=max_elapsed_time,
            next_relationship_type=next_relationship_type,
            num_elapsed_times=num_elapsed_times,
            output_time=output_time,
            output_time_property=output_time_property,
            random_seed=random_seed,
            relationship_types=relationship_types,
            smoothing_rate=smoothing_rate,
            smoothing_window=smoothing_window,
            time_node_property=time_node_property,
        )

        with _translate_feature_not_enabled():
            result = self._node_property_endpoints.run_job_and_mutate(FAST_PATH_ENDPOINT, config, mutate_property)

        return FastPathMutateResult(**result)

    def stream(
        self,
        G: Graph,
        base_node_label: str,
        event_node_label: str,
        dimension: int,
        max_elapsed_time: int,
        num_elapsed_times: int,
        *,
        categorical_event_properties: list[str] = [],
        relationship_types: list[str] = ALL_TYPES,
        context_node_label: str | None = None,
        decay_factor: float = 1.0,
        event_features: str | None = None,
        first_relationship_type: str | None = None,
        ignored_event_category: int = -1,
        next_relationship_type: str | None = None,
        output_time: float | None = None,
        output_time_property: str | None = None,
        random_seed: Any | None = None,
        smoothing_rate: float = 0.0,
        smoothing_window: int = 0,
        time_node_property: str | None = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            base_node_label=base_node_label,
            categorical_event_properties=categorical_event_properties,
            context_node_label=context_node_label,
            decay_factor=decay_factor,
            dimension=dimension,
            event_features=event_features,
            event_node_label=event_node_label,
            first_relationship_type=first_relationship_type,
            ignored_event_category=ignored_event_category,
            max_elapsed_time=max_elapsed_time,
            next_relationship_type=next_relationship_type,
            num_elapsed_times=num_elapsed_times,
            output_time=output_time,
            output_time_property=output_time_property,
            random_seed=random_seed,
            relationship_types=relationship_types,
            smoothing_rate=smoothing_rate,
            smoothing_window=smoothing_window,
            time_node_property=time_node_property,
        )

        with _translate_feature_not_enabled():
            return self._node_property_endpoints.run_job_and_stream(FAST_PATH_ENDPOINT, G, config)

    def write(
        self,
        G: Graph,
        base_node_label: str,
        event_node_label: str,
        write_property: str,
        dimension: int,
        max_elapsed_time: int,
        num_elapsed_times: int,
        *,
        categorical_event_properties: list[str] = [],
        relationship_types: list[str] = ALL_TYPES,
        context_node_label: str | None = None,
        decay_factor: float = 1.0,
        event_features: str | None = None,
        first_relationship_type: str | None = None,
        ignored_event_category: int = -1,
        next_relationship_type: str | None = None,
        output_time: float | None = None,
        output_time_property: str | None = None,
        random_seed: Any | None = None,
        smoothing_rate: float = 0.0,
        smoothing_window: int = 0,
        time_node_property: str | None = None,
        write_concurrency: int | None = None,
    ) -> FastPathWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            base_node_label=base_node_label,
            categorical_event_properties=categorical_event_properties,
            context_node_label=context_node_label,
            decay_factor=decay_factor,
            dimension=dimension,
            event_features=event_features,
            event_node_label=event_node_label,
            first_relationship_type=first_relationship_type,
            ignored_event_category=ignored_event_category,
            max_elapsed_time=max_elapsed_time,
            next_relationship_type=next_relationship_type,
            num_elapsed_times=num_elapsed_times,
            output_time=output_time,
            output_time_property=output_time_property,
            random_seed=random_seed,
            relationship_types=relationship_types,
            smoothing_rate=smoothing_rate,
            smoothing_window=smoothing_window,
            time_node_property=time_node_property,
        )

        with _translate_feature_not_enabled():
            result = self._node_property_endpoints.run_job_and_write(
                FAST_PATH_ENDPOINT,
                G,
                config,
                property_overwrites=write_property,
                write_concurrency=write_concurrency,
            )

        return FastPathWriteResult(**result)
