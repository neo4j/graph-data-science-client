from pandas import DataFrame

from graphdatascience import QueryRunner
from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.relationships_endpoints import (
    Aggregation,
    RelationshipsDropResult,
    RelationshipsEndpoints,
    RelationshipsInverseIndexResult,
    RelationshipsToUndirectedResult,
    RelationshipsWriteResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_TYPES
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.gds_arrow_client import GdsArrowClient


class RelationshipCypherEndpoints(RelationshipsEndpoints):
    def __init__(self, query_runner: QueryRunner, gds_arrow_client: GdsArrowClient | None = None):
        self._query_runner = query_runner
        self._gds_arrow_client = gds_arrow_client

    def stream(
        self,
        G: GraphV2,
        relationship_types: list[str] = ALL_TYPES,
        relationship_properties: list[str] | None = None,
        *,
        concurrency: int | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
    ) -> DataFrame:
        effective_rel_types = relationship_types if relationship_types is not None else ["*"]

        if self._gds_arrow_client is not None:
            database = self._query_runner.database()
            if database is None:
                raise ValueError("The database is not set")

            if relationship_properties:
                return self._gds_arrow_client.get_relationship_properties(
                    G.name(),
                    database,
                    relationship_properties,
                    effective_rel_types,
                    concurrency,
                )
            else:
                return self._gds_arrow_client.get_relationships(G.name(), database, effective_rel_types, concurrency)
        else:
            config = ConfigConverter.convert_to_gds_config(
                concurrency=concurrency,
                sudo=sudo,
                log_progress=log_progress,
                username=username,
            )

            if not relationship_properties:
                endpoint = "gds.graph.relationships.stream"
                params = CallParameters(
                    graph_name=G.name(),
                    relationship_types=effective_rel_types,
                    config=config,
                )
            elif len(relationship_properties) == 1:
                endpoint = "gds.graph.relationshipProperty.stream"
                params = CallParameters(
                    graph_name=G.name(),
                    relationship_property=relationship_properties[0],
                    relationship_types=effective_rel_types,
                    config=config,
                )
            else:
                endpoint = "gds.graph.relationshipProperties.stream"
                params = CallParameters(
                    graph_name=G.name(),
                    relationship_properties=relationship_properties,
                    relationship_types=effective_rel_types,
                    config=config,
                )

            result = self._query_runner.call_procedure(endpoint=endpoint, params=params)

            if relationship_properties and len(relationship_properties) == 1:
                result = result.rename(columns={"propertyValue": relationship_properties[0]})

            return result

    def write(
        self,
        G: GraphV2,
        relationship_type: str,
        relationship_properties: list[str] | None = None,
        *,
        concurrency: int | None = None,
        write_concurrency: int | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        job_id: str | None = None,
    ) -> RelationshipsWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            job_id=job_id,
        )

        if relationship_properties and len(relationship_properties) > 1:
            endpoint = "gds.graph.relationshipProperties.write"
            params = CallParameters(
                graph_name=G.name(),
                relationship_type=relationship_type,
                relationship_properties=relationship_properties,
                config=config,
            )
        else:
            endpoint = "gds.graph.relationship.write"
            params = CallParameters(
                graph_name=G.name(),
                relationship_type=relationship_type,
                relationship_property=relationship_properties[0] if relationship_properties else None,
                config=config,
            )

        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint=endpoint, params=params, logging=log_progress).squeeze()

        return RelationshipsWriteResult(**result.to_dict())

    def drop(
        self,
        G: GraphV2,
        relationship_type: str,
        *,
        fail_if_missing: bool = True,
    ) -> RelationshipsDropResult:
        if relationship_type not in G.relationship_types() and fail_if_missing:
            raise ValueError(f"Relationship type '{relationship_type}' does not exist in the graph")

        params = CallParameters(
            graph_name=G.name(),
            relationship_type=relationship_type,
        )

        result = self._query_runner.call_procedure(endpoint="gds.graph.relationships.drop", params=params).squeeze()

        return RelationshipsDropResult(**result.to_dict())

    def index_inverse(
        self,
        G: GraphV2,
        relationship_types: list[str],
        *,
        concurrency: int | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        job_id: str | None = None,
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
            endpoint="gds.graph.relationships.indexInverse", params=params, logging=log_progress
        ).squeeze()

        return RelationshipsInverseIndexResult(**result.to_dict())

    def to_undirected(
        self,
        G: GraphV2,
        relationship_type: str,
        mutate_relationship_type: str,
        *,
        aggregation: Aggregation | dict[str, Aggregation] | None = None,
        concurrency: int | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        job_id: str | None = None,
    ) -> RelationshipsToUndirectedResult:
        aggregation_value: str | dict[str, str] | None = None
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
            endpoint="gds.graph.relationships.toUndirected", params=params, logging=log_progress
        ).squeeze()

        return RelationshipsToUndirectedResult(**result.to_dict())
