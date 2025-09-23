from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize_single
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.node_properties_endpoints import (
    NodePropertiesDropResult,
    NodePropertiesEndpoints,
    NodePropertiesWriteResult,
    NodePropertySpec,
)
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpoints
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.procedure_surface.utils.result_utils import join_db_node_properties


class NodePropertiesArrowEndpoints(NodePropertiesEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        query_runner: Optional[QueryRunner] = None,
        show_progress: bool = True,
    ):
        self._arrow_client = arrow_client
        self._query_runner = query_runner
        self._write_back_client: Optional[RemoteWriteBackClient] = (
            RemoteWriteBackClient(arrow_client, query_runner) if query_runner is not None else None
        )
        self._node_property_endpoints = NodePropertyEndpoints(
            arrow_client, self._write_back_client, show_progress=show_progress
        )
        self._show_progress = show_progress

    def stream(
        self,
        G: GraphV2,
        node_properties: Union[str, List[str]],
        *,
        list_node_labels: Optional[bool] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,
        db_node_properties: Optional[List[str]] = None,
    ) -> DataFrame:
        has_db_properties = (db_node_properties is not None) and (len(db_node_properties) > 0)

        if has_db_properties and self._query_runner is None:
            raise ValueError("The option `db_node_properties` is only available if a database connection is provided.")

        normalized_properties = node_properties if isinstance(node_properties, list) else [node_properties]

        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            node_properties=normalized_properties,
            list_node_labels=list_node_labels,
            node_labels=node_labels,
            concurrency=concurrency,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            job_id=job_id,
        )

        job_id = JobClient.run_job(self._arrow_client, "v2/graph.nodeProperties.stream", config)
        result = JobClient.stream_results(self._arrow_client, G.name(), job_id)

        if has_db_properties:
            return join_db_node_properties(result, db_node_properties, self._query_runner)  # type: ignore

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
        log_progress: bool = True,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,
    ) -> NodePropertiesWriteResult:
        if self._write_back_client is None:
            raise ValueError("Write back is only available if a database connection is provided.")

        node_property_spec = NodePropertySpec(node_properties)

        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            node_properties=node_property_spec.property_names(),
            node_labels=node_labels,
            concurrency=concurrency,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            job_id=job_id,
        )

        job_id = JobClient.run_job(self._arrow_client, "v2/graph.nodeProperties.stream", config)

        write_result = self._write_back_client.write(
            G.name(),
            job_id,
            concurrency=write_concurrency if write_concurrency is not None else concurrency,
            property_overwrites=node_property_spec.to_dict(),
            log_progress=self._show_progress and log_progress,
        )

        return NodePropertiesWriteResult(
            graphName=G.name(),
            nodeProperties=node_property_spec.property_names(),
            propertiesWritten=write_result.written_node_properties,
            writeMillis=write_result.write_millis,
            configuration=config,
        )

    def drop(
        self,
        G: GraphV2,
        node_properties: List[str],
        *,
        fail_if_missing: Optional[bool] = None,
        concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,
    ) -> NodePropertiesDropResult:
        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            node_properties=node_properties,
            fail_if_missing=fail_if_missing,
            concurrency=concurrency,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            job_id=job_id,
        )
        result = self._arrow_client.do_action_with_retry("v2/graph.nodeProperties.drop", config)
        deserialized_result = deserialize_single(result)

        return NodePropertiesDropResult(**deserialized_result)
