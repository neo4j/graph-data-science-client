from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Tuple

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.protocol.arrow_config import build_arrow_config
from graphdatascience.query_runner.protocol.status import Status
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.query_type import QueryType
from graphdatascience.query_runner.termination_flag import TerminationFlag
from graphdatascience.session.dbms.protocol_version import ProtocolVersion


class ProjectProtocol(ABC):
    def __init__(
        self, arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, termination_flag: TerminationFlag
    ):
        self._arrow_client = arrow_client
        self._query_runner = query_runner
        self._termination_flag = termination_flag

    @abstractmethod
    def start_cypher_projection(
        self,
        graph_name: str,
        query: str,
        job_id: str,
        query_parameters: dict[str, Any] | None = None,
        concurrency: int | None = None,
        undirected_relationship_types: list[str] | None = None,
        inverse_indexed_relationship_types: list[str] | None = None,
        batch_size: int | None = None,
    ) -> Tuple[str, QueryRunner]:
        """Kick off a cypher projection without polling for completion.

        Returns the job id to poll the Arrow job status with.
        """
        pass

    @abstractmethod
    def start_store_projection(
        self,
        graph_name: str,
        node_label_filter: list[str],
        relationship_type_filter: list[str],
        node_properties: list[str] | None = None,
        relationship_properties: list[str] | None = None,
        job_id: str | None = None,
        concurrency: int | None = None,
        undirected_relationship_types: list[str] | None = None,
        inverse_indexed_relationship_types: list[str] | None = None,
        batch_size: int | None = None,
    ) -> Tuple[str, QueryRunner]:
        """Kick off a native store projection without polling for completion.

        Returns the server-assigned job id.
        """
        pass

    @abstractmethod
    def get_status(
        self,
        job_id: str,
        query_runner: QueryRunner,
    ) -> dict[str, Any]:
        pass

    @staticmethod
    def select(
        protocol_version: ProtocolVersion,
        arrow_client: AuthenticatedArrowClient,
        query_runner: QueryRunner,
        termination_flag: TerminationFlag,
    ) -> "ProjectProtocol":
        return {
            ProtocolVersion.V3: ProjectProtocolV3(arrow_client, query_runner, termination_flag),
            ProtocolVersion.V4: ProjectProtocolV4(arrow_client, query_runner, termination_flag),
        }[protocol_version]


class ProjectProtocolV3(ProjectProtocol):
    def __init__(
        self, arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, termination_flag: TerminationFlag
    ):
        super().__init__(arrow_client, query_runner, termination_flag)
        self._parameter_cache: dict[str, CallParameters] = {}
        self._result_cache: dict[str, dict[str, Any]] = {}

    def start_cypher_projection(
        self,
        graph_name: str,
        query: str,
        job_id: str,
        query_parameters: dict[str, Any] | None = None,
        concurrency: int | None = None,
        undirected_relationship_types: list[str] | None = None,
        inverse_indexed_relationship_types: list[str] | None = None,
        batch_size: int | None = None,
    ) -> Tuple[str, QueryRunner]:
        self._result_cache.pop(job_id, None)

        configuration = ConfigConverter.convert_to_gds_config(
            queryParameters=query_parameters,
            undirectedRelationshipTypes=undirected_relationship_types,
            inverseIndexedRelationshipTypes=inverse_indexed_relationship_types,
            concurrency=concurrency,
        )

        params = {
            "graph_name": graph_name,
            "query": query,
            "jobId": job_id,
            "configuration": configuration,
            "arrow_config": build_arrow_config(self._arrow_client, batch_size),
        }

        self._parameter_cache[job_id] = CallParameters(params)

        response = self._query_runner.run_cypher(
            "CALL gds.arrow.project.v3($graph_name, $query, $jobId, $arrow_config, $configuration)",
            QueryType.USER_TRANSPILED,
            params,
        ).squeeze()

        member_host = response["host"]
        member_port = response["port"] if ("port" in response.index) else 7687

        projection_query_runner = self._query_runner.cloneWithoutRouting(member_host, member_port)

        return job_id, projection_query_runner

    def start_store_projection(
        self,
        graph_name: str,
        node_label_filter: list[str],
        relationship_type_filter: list[str],
        node_properties: list[str] | None = None,
        relationship_properties: list[str] | None = None,
        job_id: str | None = None,
        concurrency: int | None = None,
        undirected_relationship_types: list[str] | None = None,
        inverse_indexed_relationship_types: list[str] | None = None,
        batch_size: int | None = None,
    ) -> Tuple[str, QueryRunner]:
        raise NotImplementedError("Store projection is not supported by the databases")

    def get_status(self, job_id: str, query_runner: QueryRunner) -> dict[str, Any]:
        if self._result_cache.get(job_id) is not None:
            return self._result_cache[job_id]

        if job_id not in self._parameter_cache:
            # This means that the protocol was recreated, and the job_id is no longer in the cache.
            # There is nothing we can do about this.
            raise Exception(f"Status for projection with id {job_id} cannot be retrieved")

        params = self._parameter_cache[job_id]

        self._termination_flag.assert_running()
        projection_result: dict[str, Any] = (
            query_runner.run_cypher(
                "CALL gds.arrow.project.v3($graph_name, $query, $jobId, $arrow_config, $configuration)",
                QueryType.USER_TRANSPILED,
                params,
            )
            .squeeze()
            .to_dict()
        )

        if projection_result["status"] == Status.DONE.name:
            self._result_cache[job_id] = projection_result

        return projection_result


class ProjectProtocolV4(ProjectProtocol):
    def start_cypher_projection(
        self,
        graph_name: str,
        query: str,
        job_id: str,
        query_parameters: dict[str, Any] | None = None,
        concurrency: int | None = None,
        undirected_relationship_types: list[str] | None = None,
        inverse_indexed_relationship_types: list[str] | None = None,
        batch_size: int | None = None,
    ) -> Tuple[str, QueryRunner]:
        configuration = ConfigConverter.convert_to_gds_config(
            queryParameters=query_parameters,
            undirectedRelationshipTypes=undirected_relationship_types,
            inverseIndexedRelationshipTypes=inverse_indexed_relationship_types,
            concurrency=concurrency,
        )

        params = {
            "graph_name": graph_name,
            "query": query,
            "jobId": job_id,
            "configuration": configuration,
            "arrow_config": build_arrow_config(self._arrow_client, batch_size),
        }

        actual_job_id, projection_query_runner = self._start_job(
            "CALL gds.arrow.project.cypher.v4($graph_name, $query, $jobId, $arrow_config, $configuration)",
            params,
        )

        return actual_job_id, projection_query_runner

    def start_store_projection(
        self,
        graph_name: str,
        node_label_filter: list[str],
        relationship_type_filter: list[str],
        node_properties: list[str] | None = None,
        relationship_properties: list[str] | None = None,
        job_id: str | None = None,
        concurrency: int | None = None,
        undirected_relationship_types: list[str] | None = None,
        inverse_indexed_relationship_types: list[str] | None = None,
        batch_size: int | None = None,
    ) -> Tuple[str, QueryRunner]:
        configuration = ConfigConverter.convert_to_gds_config(
            nodeProperties=node_properties,
            relationshipProperties=relationship_properties,
            job_id=job_id,
            undirectedRelationshipTypes=undirected_relationship_types,
            inverseIndexedRelationshipTypes=inverse_indexed_relationship_types,
            readConcurrency=concurrency,
        )

        params = {
            "graph_name": graph_name,
            "node_labels": node_label_filter,
            "relationship_types": relationship_type_filter,
            "configuration": configuration,
            "arrow_config": build_arrow_config(self._arrow_client, batch_size),
        }

        actual_job_id, projection_query_runner = self._start_job(
            "CALL gds.arrow.project.store.v4($graph_name, $node_labels, $relationship_types, $arrow_config, $configuration)",
            params,
        )

        return actual_job_id, projection_query_runner

    def get_status(self, job_id: str, query_runner: QueryRunner) -> dict[str, Any]:
        self._termination_flag.assert_running()

        status_result: dict[str, Any] = (
            query_runner.run_cypher(
                f"CALL gds.arrow.job.status.v4('{job_id}')",
                QueryType.USER_TRANSPILED,
            )
            .squeeze()
            .to_dict()
        )

        if status_result["error"] is not None:
            raise Exception(status_result["error"])

        return status_result

    def _start_job(self, query: str, params: dict[str, Any]) -> Tuple[str, QueryRunner]:
        start_response = self._query_runner.run_cypher(
            query,
            QueryType.USER_TRANSPILED,
            params=params,
        ).squeeze()

        actual_job_id = start_response["jobId"]

        member_host = start_response["host"]
        member_port = start_response["port"] if ("port" in start_response.index) else 7687
        projection_query_runner = self._query_runner.cloneWithoutRouting(member_host, member_port)

        return actual_job_id, projection_query_runner
