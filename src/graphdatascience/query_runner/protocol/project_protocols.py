from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures.thread import ThreadPoolExecutor
from logging import DEBUG, getLogger
from typing import Any, Tuple

from pandas import DataFrame, Series
from tenacity import retry, retry_if_result

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.protocol.arrow_config import build_arrow_config
from graphdatascience.query_runner.protocol.status import Status
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.query_type import QueryType
from graphdatascience.query_runner.termination_flag import TerminationFlag
from graphdatascience.retry_utils.retry_utils import before_log, job_wait_strategy
from graphdatascience.session.dbms.protocol_version import ProtocolVersion


class ProjectProtocol(ABC):
    def __init__(
        self, arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, termination_flag: TerminationFlag
    ):
        self._arrow_client = arrow_client
        self._query_runner = query_runner
        self._termination_flag = termination_flag

    @abstractmethod
    def run_cypher_projection(
        self,
        graph_name: str,
        query: str,
        job_id: str,
        query_parameters: dict[str, Any] | None = None,
        concurrency: int | None = None,
        undirected_relationship_types: list[str] | None = None,
        inverse_indexed_relationship_types: list[str] | None = None,
        batch_size: int | None = None,
        logging: bool = True,
    ) -> dict[str, Any]:
        pass

    @abstractmethod
    def run_store_projection(
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
        logging: bool = True,
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

    def _show_progress(self, job_id: str, show_progress: bool, termination_flag: TerminationFlag) -> None:
        job_client = JobClient()

        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(
                job_client.wait_for_job, self._arrow_client, job_id, show_progress, Status.DONE.name, termination_flag
            )


class ProjectProtocolV3(ProjectProtocol):
    def run_cypher_projection(
        self,
        graph_name: str,
        query: str,
        job_id: str,
        query_parameters: dict[str, Any] | None = None,
        concurrency: int | None = None,
        undirected_relationship_types: list[str] | None = None,
        inverse_indexed_relationship_types: list[str] | None = None,
        batch_size: int | None = None,
        logging: bool = True,
    ) -> dict[str, Any]:
        def is_not_done(result: DataFrame) -> bool:
            status: str = result.squeeze()["status"]
            return status != Status.DONE.name

        logger = getLogger()

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

        # We need to pin the driver to a specific cluster member
        response = self._query_runner.run_cypher(
            "CALL gds.arrow.project.v3($graph_name, $query, $jobId, $arrow_config, $configuration)",
            QueryType.USER_TRANSPILED,
            params,
        ).squeeze()

        member_host = response["host"]
        member_port = response["port"] if ("port" in response.index) else 7687
        projection_query_runner = self._query_runner.cloneWithoutRouting(member_host, member_port)

        @retry(
            reraise=True,
            before=before_log(f"Projection (graph: `{params['graph_name']}`)", logger, DEBUG),
            retry=retry_if_result(is_not_done),
            wait=job_wait_strategy(),
        )
        def project_fn() -> DataFrame:
            self._termination_flag.assert_running()
            return projection_query_runner.run_cypher(
                "CALL gds.arrow.project.v3($graph_name, $query, $jobId, $arrow_config, $configuration)",
                QueryType.USER_TRANSPILED,
                params,
            )

        self._show_progress(job_id, logging, self._termination_flag)

        projection_result = project_fn()

        projection_query_runner.close()

        return projection_result.squeeze().to_dict()  # type: ignore

    def run_store_projection(
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
        logging: bool = True,
    ) -> dict[str, Any]:
        raise NotImplementedError("Store projection is not supported in protocol version 3")


class ProjectProtocolV4(ProjectProtocol):
    def run_cypher_projection(
        self,
        graph_name: str,
        query: str,
        job_id: str,
        query_parameters: dict[str, Any] | None = None,
        concurrency: int | None = None,
        undirected_relationship_types: list[str] | None = None,
        inverse_indexed_relationship_types: list[str] | None = None,
        batch_size: int | None = None,
        logging: bool = True,
    ) -> dict[str, Any]:
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

        return self._await_completion(actual_job_id, projection_query_runner, logging)

    def run_store_projection(
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
        logging: bool = True,
    ) -> dict[str, Any]:
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

        return self._await_completion(actual_job_id, projection_query_runner, logging)

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

    def _await_completion(self, job_id: str, query_runner: QueryRunner, show_progress: bool) -> dict[str, Any]:
        def is_not_done(r: Series[Any]) -> bool:
            status: str = r["status"]
            return status != Status.DONE.name

        @retry(
            reraise=True,
            before=before_log(f"Awaiting completion for job {job_id}", getLogger(), DEBUG),
            retry=retry_if_result(is_not_done),
            wait=job_wait_strategy(),
        )
        def poll_result() -> Series[Any]:
            self._termination_flag.assert_running()
            status_result = query_runner.run_cypher(
                f"CALL gds.arrow.job.status.v4('{job_id}')",
                QueryType.USER_TRANSPILED,
            ).squeeze()

            if status_result["error"] is not None:
                raise Exception(status_result["error"])

            return status_result  # type: ignore

        self._show_progress(job_id, show_progress, self._termination_flag)

        result = poll_result()

        query_runner.close()

        return result["result"]  # type: ignore
