from concurrent.futures.thread import ThreadPoolExecutor
from logging import DEBUG, getLogger
from typing import Any

from tenacity import retry, retry_if_result

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.query_runner.protocol.project_protocols import ProjectProtocol
from graphdatascience.query_runner.protocol.status import Status
from graphdatascience.query_runner.termination_flag import TerminationFlag
from graphdatascience.retry_utils.retry_utils import before_log, job_wait_strategy


class ProjectionRunner:
    def __init__(
        self,
        project_protocol: ProjectProtocol,
        arrow_client: AuthenticatedArrowClient,
        termination_flag: TerminationFlag,
    ):
        self._project_protocol = project_protocol
        self._arrow_client = arrow_client
        self._termination_flag = termination_flag

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
        show_progress: bool = True,
    ) -> dict[str, Any]:
        actual_job_id, query_runner = self._project_protocol.start_cypher_projection(
            graph_name,
            query,
            job_id,
            query_parameters,
            concurrency,
            undirected_relationship_types,
            inverse_indexed_relationship_types,
            batch_size,
        )

        return self._await_result(job_id, query_runner, show_progress)

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
        show_progress: bool = True,
    ) -> dict[str, Any]:
        actual_job_id, query_runner = self._project_protocol.start_store_projection(
            graph_name,
            node_label_filter,
            relationship_type_filter,
            node_properties,
            relationship_properties,
            job_id,
            concurrency,
            undirected_relationship_types,
            inverse_indexed_relationship_types,
            batch_size,
        )

        result = self._await_result(actual_job_id, query_runner, show_progress)

        return result["result"]  # type: ignore

    def _await_result(self, job_id: str, query_runner: QueryRunner, show_progress: bool) -> dict[str, Any]:
        def is_not_done(r: dict[str, Any]) -> bool:
            status: str = r["status"]
            return status != Status.DONE.name

        @retry(
            reraise=True,
            before=before_log(f"Awaiting completion for job {job_id}", getLogger(), DEBUG),
            retry=retry_if_result(is_not_done),
            wait=job_wait_strategy(),
        )
        def poll_result() -> dict[str, Any]:
            self._termination_flag.assert_running()
            return self._project_protocol.get_status(job_id, query_runner)

        self._show_progress(job_id, show_progress, self._termination_flag)

        try:
            result = poll_result()
        finally:
            query_runner.close()

        return result

    def _show_progress(self, job_id: str, show_progress: bool, termination_flag: TerminationFlag) -> None:
        job_client = JobClient()

        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(
                job_client.wait_for_job, self._arrow_client, job_id, show_progress, Status.DONE.name, termination_flag
            )
