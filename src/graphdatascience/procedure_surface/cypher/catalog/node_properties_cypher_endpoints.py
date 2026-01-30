from pandas import DataFrame

from graphdatascience.arrow_client.v1.gds_arrow_client import GdsArrowClient
from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.node_properties_endpoints import (
    NodePropertiesDropResult,
    NodePropertiesEndpoints,
    NodePropertiesWriteResult,
    NodePropertySpec,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS
from graphdatascience.procedure_surface.cypher.catalog.utils import require_database
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.procedure_surface.utils.result_utils import join_db_node_properties, transpose_property_columns
from graphdatascience.query_runner.query_runner import QueryRunner


class NodePropertiesCypherEndpoints(NodePropertiesEndpoints):
    def __init__(self, query_runner: QueryRunner, gds_arrow_client: GdsArrowClient | None = None):
        self._query_runner = query_runner
        self._gds_arrow_client = gds_arrow_client

    def stream(
        self,
        G: GraphV2,
        node_properties: str | list[str],
        *,
        list_node_labels: bool | None = None,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        job_id: str | None = None,  # setting the job id is not supported by the Cypher procedure
        db_node_properties: list[str] | None = None,
    ) -> DataFrame:
        if self._gds_arrow_client is not None:
            database = require_database(self._query_runner)

            result = self._gds_arrow_client.get_node_properties(
                G.name(), database, node_properties, node_labels, list_node_labels or False, concurrency
            )
        else:
            config = ConfigConverter.convert_to_gds_config(
                list_node_labels=list_node_labels,
                concurrency=concurrency,
                sudo=sudo,
                log_progress=log_progress,
                username=username,
            )

            params = CallParameters(
                graph_name=G.name(),
                node_properties=node_properties,
                node_labels=node_labels if node_labels is not None else ["*"],
                config=config,
            )

            raw_result = self._query_runner.call_procedure(endpoint="gds.graph.nodeProperties.stream", params=params)
            result = transpose_property_columns(raw_result, list_node_labels or False)

        if (db_node_properties is not None) and (len(db_node_properties) > 0):
            return join_db_node_properties(result, db_node_properties, self._query_runner)
        else:
            return result

    def write(
        self,
        G: GraphV2,
        node_properties: str | list[str] | dict[str, str],
        *,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
        write_concurrency: int | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        job_id: str | None = None,
    ) -> NodePropertiesWriteResult:
        node_property_spec = NodePropertySpec(node_properties)

        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            write_concurrency=write_concurrency,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            job_id=job_id,
        )

        params = CallParameters(
            graph_name=G.name(),
            node_properties=node_property_spec.to_dict(),
            node_labels=node_labels if node_labels is not None else ["*"],
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.graph.nodeProperties.write", params=params, logging=log_progress
        ).squeeze()

        return NodePropertiesWriteResult(**result.to_dict())

    def drop(
        self,
        G: GraphV2,
        node_properties: list[str],
        *,
        fail_if_missing: bool | None = None,
        concurrency: int | None = None,
        username: str | None = None,
    ) -> NodePropertiesDropResult:
        config = ConfigConverter.convert_to_gds_config(
            fail_if_missing=fail_if_missing,
            concurrency=concurrency,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), node_properties=node_properties, config=config)

        result = self._query_runner.call_procedure(endpoint="gds.graph.nodeProperties.drop", params=params).squeeze()

        return NodePropertiesDropResult(**result.to_dict())
