from __future__ import annotations

import builtins
from uuid import uuid4

from tenacity import Retrying, retry_if_result

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.query_runner import QueryType
from graphdatascience.query_runner.termination_flag import TerminationFlag
from graphdatascience.retry_utils.retry_utils import job_wait_strategy


class RemoteStoreProjection:
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        query_runner: QueryRunner,
        show_progress: bool = False,
        termination_flag: TerminationFlag = TerminationFlag.create(),
    ):
        self._arrow_client = arrow_client
        self._query_runner = query_runner
        self._show_progress = show_progress
        self._termination_flag = termination_flag

    def run_projection(
        self,
        graph_name: str,
        node_label_filter: list[str],
        relationship_type_filter: list[str],
        *,
        node_properties: list[str] | None = None,
        relationship_properties: list[str] | None = None,
        job_id: str | None = None,
        concurrency: int | None = None,
        undirected_relationship_types: builtins.list[str] | None = None,
        inverse_indexed_relationship_types: builtins.list[str] | None = None,
        batch_size: int | None = None,
        logging: bool = True,
    ) -> StoreProjectionResult:
        resolved_job_id: str = job_id if job_id is not None else str(uuid4())

        projection_query = """
            WITH gds.aura.api.credentials($clientId, $clientSecret) AS credentials
            CALL gds.graph.project(
                $graphName,
                $nodeProjection,
                $relationshipProjection,
                $config
            ) YIELD
                graphName,
                nodeCount,
                relationshipCount,
                projectMillis
            RETURN *
        """

        apoc_periodic_query = """
            CALL apoc.periodic.submit(
                $jobId,
                $projectionQuery,
                $params
            )
        """

        params = {
            "graphName": graph_name,
            "nodeProjection": node_label_filter,
            "relationshipProjection": relationship_type_filter,
            "config": {"job_id": resolved_job_id},
        }

        self._query_runner.run_cypher(
            apoc_periodic_query,
            QueryType.USER_TRANSPILED,
            {"jobId": resolved_job_id, "projectionQuery": projection_query, "params": params},
        )

        self._await_projection(resolved_job_id)

        return StoreProjectionResult(**JobClient.get_summary(self._arrow_client, resolved_job_id))

    def _await_projection(self, job_id: str) -> None:
        for attempt in Retrying(retry=retry_if_result(lambda _: True), wait=job_wait_strategy(), reraise=True):
            with attempt:
                self._termination_flag.assert_running()

                status_query = """
                    CALL apoc.periodic.list() 
                    YIELD name, done, cancelled
                    WHERE name = $jobId
                    LIMIT 1
                    RETURN done, cancelled
                """

                result = self._query_runner.run_cypher(
                    status_query, QueryType.USER_TRANSPILED, {"jobId": job_id}
                ).squeeze()

                if result["cancelled"]:
                    raise RuntimeError("Projection job was cancelled")

                if result["done"]:
                    return


class StoreProjectionResult(BaseResult):
    """Result object for graph projection jobs."""

    graph_name: str
    node_count: int
    relationship_count: int
