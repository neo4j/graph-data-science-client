from typing import Any, Optional

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.node_label_endpoints import (
    NodeLabelEndpoints,
    NodeLabelMutateResult,
    NodeLabelWriteResult,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class NodeLabelCypherEndpoints(NodeLabelEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        node_label: str,
        *,
        node_filter: str,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> NodeLabelMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            node_filter=node_filter,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            write_concurrency=write_concurrency,
            job_id=job_id,
        )

        params = CallParameters(graph_name=G.name(), node_label=node_label, config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.graph.nodeLabel.mutate", params=params
        ).squeeze()

        return NodeLabelMutateResult(**cypher_result.to_dict())

    def write(
        self,
        G: GraphV2,
        node_label: str,
        *,
        node_filter: str,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> NodeLabelWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            node_filter=node_filter,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            write_concurrency=write_concurrency,
            job_id=job_id,
        )

        params = CallParameters(graph_name=G.name(), node_label=node_label, config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(endpoint="gds.graph.nodeLabel.write", params=params).squeeze()

        return NodeLabelWriteResult(**cypher_result.to_dict())
