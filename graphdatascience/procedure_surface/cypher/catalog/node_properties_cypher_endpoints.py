from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.node_properties_endpoints import (
    NodePropertiesDropResult,
    NodePropertiesEndpoints,
    NodePropertiesWriteResult,
    NodePropertySpec,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.procedure_surface.utils.result_utils import join_db_node_properties, transpose_property_columns
from graphdatascience.query_runner.gds_arrow_client import GdsArrowClient
from graphdatascience.query_runner.query_runner import QueryRunner


class NodePropertiesCypherEndpoints(NodePropertiesEndpoints):
    def __init__(self, query_runner: QueryRunner, gds_arrow_client: Optional[GdsArrowClient] = None):
        self._query_runner = query_runner
        self._gds_arrow_client = gds_arrow_client

    def stream(
        self,
        G: GraphV2,
        node_properties: Union[str, List[str]],
        *,
        list_node_labels: Optional[bool] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,  # setting the job id is not supported by the Cypher procedure
        db_node_properties: Optional[List[str]] = None,
    ) -> DataFrame:
        if self._gds_arrow_client is not None:
            database = self._query_runner.database()
            if database is None:
                raise ValueError("The database is not set")

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
        node_properties: Union[str, List[str], dict[str, str]],
        *,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,
    ) -> NodePropertiesWriteResult:
        node_property_spec = NodePropertySpec(node_properties)

        config = ConfigConverter.convert_to_gds_config(
            node_labels=node_labels,
            concurrency=concurrency,
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

        result = self._query_runner.call_procedure(endpoint="gds.graph.nodeProperties.write", params=params).squeeze()

        return NodePropertiesWriteResult(**result.to_dict())

    def drop(
        self,
        G: GraphV2,
        node_properties: List[str],
        *,
        fail_if_missing: Optional[bool] = None,
        concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,
    ) -> NodePropertiesDropResult:
        config = ConfigConverter.convert_to_gds_config(
            fail_if_missing=fail_if_missing,
            concurrency=concurrency,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), node_properties=node_properties, config=config)

        result = self._query_runner.call_procedure(endpoint="gds.graph.nodeProperties.drop", params=params).squeeze()

        return NodePropertiesDropResult(**result.to_dict())
