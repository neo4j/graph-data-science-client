from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience import Graph, QueryRunner
from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.relationships_endpoints import (
    Aggregation,
    RelationshipsDropResult,
    RelationshipsEndpoints,
    RelationshipsInverseIndexResult,
    RelationshipsToUndirectedResult,
    RelationshipsWriteResult,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.gds_arrow_client import GdsArrowClient


class RelationshipCypherEndpoints(RelationshipsEndpoints):
    def __init__(self, query_runner: QueryRunner, gds_arrow_client: Optional[GdsArrowClient] = None):
        self._query_runner = query_runner
        self._gds_arrow_client = gds_arrow_client

    def stream(
        self,
        G: Graph,
        relationship_types: Optional[List[str]] = None,
        *,
        concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,  # setting the job id is not supported by the Cypher procedure
    ) -> DataFrame:
        if self._gds_arrow_client is not None:
            database = self._query_runner.database()
            if database is None:
                raise ValueError("The database is not set")

            return self._gds_arrow_client.get_relationships(
                G.name(), database, relationship_types or ["*"], concurrency
            )
        else:
            config = ConfigConverter.convert_to_gds_config(
                concurrency=concurrency,
                sudo=sudo,
                log_progress=log_progress,
                username=username,
            )

            params = CallParameters(
                graph_name=G.name(),
                relationship_types=relationship_types if relationship_types is not None else ["*"],
                config=config,
            )

            return self._query_runner.call_procedure(endpoint="gds.graph.relationships.stream", params=params)

    def write(
        self,
        G: Graph,
        relationship_type: str,
        *,
        concurrency: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,
    ) -> RelationshipsWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            job_id=job_id,
        )

        params = CallParameters(
            graph_name=G.name(),
            relationship_type=relationship_type,
            relationship_property=None,
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.graph.relationship.write", params=params).squeeze()

        return RelationshipsWriteResult(**result.to_dict())

    def drop(
        self,
        G: Graph,
        relationship_type: str,
    ) -> RelationshipsDropResult:
        params = CallParameters(
            graph_name=G.name(),
            relationship_type=relationship_type,
        )

        result = self._query_runner.call_procedure(endpoint="gds.graph.relationships.drop", params=params).squeeze()

        return RelationshipsDropResult(**result.to_dict())

    def index_inverse(
        self,
        G: Graph,
        relationship_types: list[str],
        *,
        concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,
    ) -> RelationshipsInverseIndexResult:
        config = ConfigConverter.convert_to_gds_config(
            relationship_types=relationship_types,
            concurrency=concurrency,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            job_id=job_id,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.graph.relationships.indexInverse", params=params
        ).squeeze()

        return RelationshipsInverseIndexResult(**result.to_dict())

    def to_undirected(
        self,
        G: Graph,
        relationship_type: str,
        mutate_relationship_type: str,
        aggregation: Optional[Union[Aggregation, dict[str, Aggregation]]] = None,
        *,
        concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,
    ) -> RelationshipsToUndirectedResult:
        aggregation_value: Optional[Union[str, dict[str, str]]] = None
        if isinstance(aggregation, Aggregation):
            aggregation_value = aggregation.name
        elif isinstance(aggregation, dict):
            aggregation_value = {k: v.name for k, v in aggregation.items()}

        config = ConfigConverter.convert_to_gds_config(
            relationship_type=relationship_type,
            mutate_relationship_type=mutate_relationship_type,
            aggregation=aggregation_value,
            concurrency=concurrency,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            job_id=job_id,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.graph.relationships.toUndirected", params=params
        ).squeeze()

        return RelationshipsToUndirectedResult(**result.to_dict())
