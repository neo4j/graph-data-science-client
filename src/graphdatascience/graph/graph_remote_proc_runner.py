from uuid import uuid4

import pandas as pd

from graphdatascience import Graph, GraphCreateResult, QueryRunner, ServerVersion
from graphdatascience.arrow_client.v2.gds_arrow_client import GdsArrowClient
from graphdatascience.graph.base_graph_proc_runner import BaseGraphProcRunner
from graphdatascience.procedure_surface.arrow.error_handler import handle_flight_error
from graphdatascience.query_runner.protocol.project_protocols import ProjectProtocol
from graphdatascience.query_runner.termination_flag import TerminationFlag


class GraphRemoteProcRunner(BaseGraphProcRunner):
    def __init__(
        self, query_runner: QueryRunner, arrow_client: GdsArrowClient, namespace: str, server_version: ServerVersion
    ):
        super().__init__(query_runner, namespace, server_version)
        self._arrow_client = arrow_client

    # The parameters need to be in camelCase to not break the api
    def project(
        self,
        graph_name: str,
        query: str,
        job_id: str | None = None,
        concurrency: int = 4,
        undirected_relationship_types: list[str] | None = None,
        inverse_indexed_relationship_types: list[str] | None = None,
        batch_size: int | None = None,
        logging: bool = True,
    ) -> GraphCreateResult:
        project_protocol = ProjectProtocol.select(
            self._resolved_protocol_version,
            self._arrow_client.flight_client(),
            self._db_query_runner,
            TerminationFlag.create(),
        )

        try:
            result = project_protocol.run_cypher_projection(
                graph_name,
                query,
                job_id or str(uuid4()),
                concurrency,
                undirected_relationship_types,
                inverse_indexed_relationship_types,
                batch_size,
                logging,
            )

            return GraphCreateResult(Graph(graph_name, self._query_runner), pd.Series(result))

        except Exception as e:
            handle_flight_error(e)
            raise e  # above should already raise
