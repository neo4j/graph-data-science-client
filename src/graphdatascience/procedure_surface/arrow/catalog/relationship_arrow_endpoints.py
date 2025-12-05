from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize_single
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.relationships_endpoints import (
    Aggregation,
    CollapsePathResult,
    RelationshipsDropResult,
    RelationshipsEndpoints,
    RelationshipsInverseIndexResult,
    RelationshipsToUndirectedResult,
    RelationshipsWriteResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_TYPES
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter


class RelationshipArrowEndpoints(RelationshipsEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None,
        show_progress: bool = False,
    ):
        self._arrow_client = arrow_client
        self._write_back_client = write_back_client
        self._show_progress = show_progress

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
        config_input = {
            "graph_name": G.name(),
            "relationship_types": relationship_types or ["*"],
            "concurrency": concurrency,
            "sudo": sudo,
            "log_progress": log_progress,
            "username": username,
        }

        endpoint = "v2/graph.relationships.stream"
        if relationship_properties and relationship_properties != []:
            config_input["relationship_properties"] = relationship_properties
            endpoint = "v2/graph.relationshipProperties.stream"

        config = ConfigConverter.convert_to_gds_config(**config_input)

        job_id = JobClient.run_job(self._arrow_client, endpoint, config)
        result = JobClient.stream_results(self._arrow_client, G.name(), job_id)

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
        if self._write_back_client is None:
            raise ValueError("Write back is only available if a database connection is provided.")

        config_input = {
            "graph_name": G.name(),
            "relationship_types": [relationship_type],
            "concurrency": concurrency,
            "sudo": sudo,
            "log_progress": log_progress,
            "username": username,
            "job_id": job_id,
        }

        endpoint = "v2/graph.relationships.stream"
        if relationship_properties and relationship_properties != []:
            config_input["relationship_properties"] = relationship_properties
            endpoint = "v2/graph.relationshipProperties.stream"

        config = ConfigConverter.convert_to_gds_config(**config_input)

        job_id = JobClient.run_job(self._arrow_client, endpoint, config)

        write_result = self._write_back_client.write(
            G.name(),
            job_id,
            concurrency=write_concurrency if write_concurrency is not None else concurrency,
            relationship_type_overwrite=relationship_type,
            log_progress=log_progress and self._show_progress,
        )

        written_relationships = (
            write_result.written_relationships if hasattr(write_result, "written_relationships") else 0
        )
        written_properties = (
            written_relationships * len(relationship_properties) if relationship_properties is not None else 0
        )
        return RelationshipsWriteResult(
            graphName=G.name(),
            relationshipType=relationship_type,
            relationshipProperties=relationship_properties if relationship_properties is not None else [],
            relationshipsWritten=written_relationships,
            propertiesWritten=written_properties,
            writeMillis=write_result.write_millis,
            configuration=config,
        )

    def drop(
        self,
        G: GraphV2,
        relationship_type: str,
        *,
        fail_if_missing: bool = True,
    ) -> RelationshipsDropResult:
        if relationship_type not in G.relationship_types() and fail_if_missing:
            raise ValueError(f"Relationship type '{relationship_type}' does not exist in graph '{G.name()}'")

        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            relationship_type=relationship_type,
        )
        result = self._arrow_client.do_action_with_retry("v2/graph.relationships.drop", config)
        deserialized_result = deserialize_single(result)

        return RelationshipsDropResult(**deserialized_result)

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
            graph_name=G.name(),
            relationship_types=relationship_types,
            concurrency=concurrency,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            job_id=job_id,
        )

        show_progress = self._show_progress and log_progress
        job_id = JobClient.run_job_and_wait(
            self._arrow_client, "v2/graph.relationships.indexInverse", config, show_progress=show_progress
        )
        result = JobClient.get_summary(self._arrow_client, job_id)
        return RelationshipsInverseIndexResult(**result)

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
        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            relationship_type=relationship_type,
            mutate_relationship_type=mutate_relationship_type,
            aggregation=aggregation,
            concurrency=concurrency,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            job_id=job_id,
        )
        show_progress = self._show_progress and log_progress

        job_id = JobClient.run_job_and_wait(
            self._arrow_client, "v2/graph.relationships.toUndirected", config, show_progress=show_progress
        )
        result = JobClient.get_summary(self._arrow_client, job_id)
        return RelationshipsToUndirectedResult(**result)

    def collapse_path(
        self,
        G: GraphV2,
        path_templates: list[list[str]],
        mutate_relationship_type: str,
        *,
        allow_self_loops: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
    ) -> CollapsePathResult:
        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            path_templates=path_templates,
            mutate_relationship_type=mutate_relationship_type,
            allow_self_loops=allow_self_loops,
            concurrency=concurrency,
            job_id=job_id,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
        )

        show_progress = self._show_progress and log_progress
        job_id = JobClient.run_job_and_wait(
            self._arrow_client, "v2/graph.relationships.collapsePath", config, show_progress=show_progress
        )

        return CollapsePathResult(**JobClient.get_summary(self._arrow_client, job_id))
