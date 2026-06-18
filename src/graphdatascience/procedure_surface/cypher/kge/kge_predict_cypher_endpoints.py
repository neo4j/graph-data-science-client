from __future__ import annotations

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_TYPES
from graphdatascience.procedure_surface.api.kge.kge_predict_endpoints import (
    KgeMutateResult,
    KgePredictEndpoints,
    KgeWriteResult,
    NodeFilter,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class KgePredictCypherEndpoints(KgePredictEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def stream(
        self,
        G: Graph,
        *,
        node_embedding_property: str,
        relationship_type_embedding: list[float],
        scoring_function: str,
        top_k: int,
        source_node_filter: NodeFilter | None = None,
        target_node_filter: NodeFilter | None = None,
        relationship_types: list[str] = ALL_TYPES,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        username: str | None = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            node_embedding_property=node_embedding_property,
            relationship_type_embedding=relationship_type_embedding,
            scoring_function=scoring_function,
            top_k=top_k,
            source_node_filter=source_node_filter,
            target_node_filter=target_node_filter,
            relationship_types=relationship_types,
            concurrency=concurrency,
            job_id=job_id,
            sudo=sudo,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(
            endpoint="gds.ml.kge.predict.stream", params=params, logging=log_progress
        )

    def mutate(
        self,
        G: Graph,
        *,
        node_embedding_property: str,
        relationship_type_embedding: list[float],
        scoring_function: str,
        top_k: int,
        mutate_relationship_type: str,
        mutate_property: str = "score",
        source_node_filter: NodeFilter | None = None,
        target_node_filter: NodeFilter | None = None,
        relationship_types: list[str] = ALL_TYPES,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        username: str | None = None,
    ) -> KgeMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            node_embedding_property=node_embedding_property,
            relationship_type_embedding=relationship_type_embedding,
            scoring_function=scoring_function,
            top_k=top_k,
            mutate_relationship_type=mutate_relationship_type,
            mutate_property=mutate_property,
            source_node_filter=source_node_filter,
            target_node_filter=target_node_filter,
            relationship_types=relationship_types,
            concurrency=concurrency,
            job_id=job_id,
            sudo=sudo,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        raw_result = self._query_runner.call_procedure(
            endpoint="gds.ml.kge.predict.mutate", params=params, logging=log_progress
        )

        return KgeMutateResult(**raw_result.iloc[0].to_dict())

    def write(
        self,
        G: Graph,
        *,
        node_embedding_property: str,
        relationship_type_embedding: list[float],
        scoring_function: str,
        top_k: int,
        write_relationship_type: str,
        write_property: str = "score",
        source_node_filter: NodeFilter | None = None,
        target_node_filter: NodeFilter | None = None,
        relationship_types: list[str] = ALL_TYPES,
        concurrency: int | None = None,
        write_concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        username: str | None = None,
    ) -> KgeWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            node_embedding_property=node_embedding_property,
            relationship_type_embedding=relationship_type_embedding,
            scoring_function=scoring_function,
            top_k=top_k,
            write_relationship_type=write_relationship_type,
            write_property=write_property,
            source_node_filter=source_node_filter,
            target_node_filter=target_node_filter,
            relationship_types=relationship_types,
            concurrency=concurrency,
            write_concurrency=write_concurrency,
            job_id=job_id,
            sudo=sudo,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        raw_result = self._query_runner.call_procedure(
            endpoint="gds.ml.kge.predict.write", params=params, logging=log_progress
        )

        return KgeWriteResult(**raw_result.iloc[0].to_dict())
