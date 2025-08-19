from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.graphsage_predict_endpoints import (
    GraphSageMutateResult,
    GraphSagePredictEndpoints,
    GraphSageWriteResult,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter

from ...graph.graph_object import Graph
from ...query_runner.query_runner import QueryRunner


class GraphSagePredictCypherEndpoints(GraphSagePredictEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def stream(self, G: Graph, **config: Any) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(**config)

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.beta.graphSage.stream", params=params)

    def write(self, G: Graph, **config: Any) -> GraphSageWriteResult:
        config = ConfigConverter.convert_to_gds_config(**config)

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        raw_result = self._query_runner.call_procedure(endpoint="gds.beta.graphSage.write", params=params)

        return GraphSageWriteResult(**raw_result.iloc[0].to_dict())

    def mutate(self, G: Graph, **config: Any) -> GraphSageMutateResult:
        config = ConfigConverter.convert_to_gds_config(**config)

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        raw_result = self._query_runner.call_procedure(endpoint="gds.beta.graphSage.mutate", params=params)

        return GraphSageMutateResult(**raw_result.iloc[0].to_dict())

    def estimate(self, G: Graph, **config: Any) -> EstimationResult:
        config = ConfigConverter.convert_to_gds_config(**config)

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        raw_result = self._query_runner.call_procedure(endpoint="gds.beta.graphSage.stream.estimate", params=params)

        return EstimationResult(**raw_result.iloc[0].to_dict())
