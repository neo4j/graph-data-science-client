from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.node_embedding.graphsage_predict_endpoints import (
    GraphSageMutateResult,
    GraphSagePredictEndpoints,
    GraphSageWriteResult,
)
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class GraphSagePredictCypherEndpoints(GraphSagePredictEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def stream(
        self,
        G: GraphV2,
        model_name: str,
        *,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        batch_size: int | None = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            modelName=model_name,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            username=username,
            logProgress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            jobId=job_id,
            batchSize=batch_size,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(
            endpoint="gds.beta.graphSage.stream", params=params, logging=log_progress
        )

    def write(
        self,
        G: GraphV2,
        model_name: str,
        write_property: str,
        *,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        concurrency: Any | None = None,
        write_concurrency: int | None = None,
        job_id: Any | None = None,
        batch_size: int | None = None,
    ) -> GraphSageWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            modelName=model_name,
            writeProperty=write_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            username=username,
            logProgress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            writeConcurrency=write_concurrency,
            jobId=job_id,
            batchSize=batch_size,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        raw_result = self._query_runner.call_procedure(
            endpoint="gds.beta.graphSage.write", params=params, logging=log_progress
        )

        return GraphSageWriteResult(**raw_result.iloc[0].to_dict())

    def mutate(
        self,
        G: GraphV2,
        model_name: str,
        mutate_property: str,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        batch_size: int | None = None,
    ) -> GraphSageMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            modelName=model_name,
            mutateProperty=mutate_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            username=username,
            logProgress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            jobId=job_id,
            batchSize=batch_size,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        raw_result = self._query_runner.call_procedure(
            endpoint="gds.beta.graphSage.mutate", params=params, logging=log_progress
        )

        return GraphSageMutateResult(**raw_result.iloc[0].to_dict())

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        model_name: str,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        batch_size: int | None = None,
        concurrency: int | None = None,
        log_progress: bool = True,
        username: str | None = None,
        sudo: bool | None = None,
        job_id: str | None = None,
    ) -> EstimationResult:
        algo_config = ConfigConverter.convert_to_gds_config(
            modelName=model_name,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            username=username,
            logProgress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            jobId=job_id,
            batchSize=batch_size,
        )

        return estimate_algorithm(
            endpoint="gds.beta.graphSage.stream.estimate",
            query_runner=self._query_runner,
            G=G,
            algo_config=algo_config,
        )
