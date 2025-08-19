from typing import Any

from pandas import DataFrame

from graphdatascience.graph.graph_object import Graph
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.graphsage_predict_endpoints import (
    GraphSageMutateResult,
    GraphSagePredictEndpoints,
    GraphSageWriteResult,
)

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from .model_api_arrow import ModelApiArrow
from .node_property_endpoints import NodePropertyEndpoints


class GraphSagePredictArrowEndpoints(GraphSagePredictEndpoints):
    def __init__(self, arrow_client: AuthenticatedArrowClient):
        self._arrow_client = arrow_client
        self._node_property_endpoints = NodePropertyEndpoints(arrow_client)
        self._model_api = ModelApiArrow(arrow_client)

    def stream(self, G: Graph, **config: Any) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(G, **config)

        return self._node_property_endpoints.run_job_and_stream("v2/embeddings.graphSage", G, config)

    def write(self, G: Graph, **config: Any) -> GraphSageWriteResult:
        config = self._node_property_endpoints.create_base_config(G, **config)

        raw_result = self._node_property_endpoints.run_job_and_write(
            "v2/embeddings.graphSage",
            G,
            config,
            config.get("writeConcurrency"),
            config.get("concurrency"),
        )

        return GraphSageWriteResult(**raw_result)

    def mutate(self, G: Graph, **config: Any) -> GraphSageMutateResult:
        config = self._node_property_endpoints.create_base_config(G, **config)

        mutateProperty = config.pop("mutateProperty", "")

        raw_result = self._node_property_endpoints.run_job_and_mutate(
            "v2/embeddings.graphSage",
            G,
            config,
            mutateProperty,
        )

        return GraphSageMutateResult(**raw_result)

    def estimate(self, G: Graph, **config: Any) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(**config)

        return self._node_property_endpoints.estimate(
            "v2/embeddings.graphSage.estimate",
            G,
            config,
        )
