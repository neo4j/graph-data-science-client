from typing import Any, Optional, Union

from pandas import DataFrame

from graphdatascience import Graph
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize_single
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.node_properties_endpoints import (
    NodePropertiesDropResult,
    NodePropertiesEndpoints,
    NodePropertiesWriteResult,
    NodePropertySpec,
)
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpoints
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter


class NodePropertiesArrowEndpoints(NodePropertiesEndpoints):
    def __init__(self, arrow_client: AuthenticatedArrowClient, write_back_client: Optional[RemoteWriteBackClient] = None):
        self._arrow_client = arrow_client
        self._write_back_client = write_back_client
        self._node_property_endpoints = NodePropertyEndpoints(arrow_client, write_back_client)

    def stream(
        self,
        G: Graph,
        node_properties: list[Union[str, NodePropertySpec]],
        *,
        list_node_labels: Optional[bool] = None,
        node_labels: Optional[list[str]] = None,
        concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,
    ) -> DataFrame:
        normalized_properties: list[NodePropertySpec] = self._normalize_node_properties(node_properties)

        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            node_properties=[p.name for p in normalized_properties],
            list_node_labels=list_node_labels,
            node_labels=node_labels,
            concurrency=concurrency,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            job_id=job_id,
        )

        job_id = JobClient.run_job(self._arrow_client, "v2/graph.nodeProperties.stream", config)
        df = JobClient.stream_results(self._arrow_client, G.name(), job_id)

        return df.rename(columns=(self._property_mapping(normalized_properties)))

    def write(
        self,
        G: Graph,
        node_properties: list[Union[str, NodePropertySpec]],
        *,
        node_labels: Optional[list[str]] = None,
        concurrency: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,
    ) -> NodePropertiesWriteResult:
        normalized_properties = self._normalize_node_properties(node_properties)

        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            node_properties=[p.name for p in normalized_properties],
            node_labels=node_labels,
            concurrency=concurrency,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            job_id=job_id,
        )

        job_id = JobClient.run_job(self._arrow_client, "v2/graph.nodeProperties.stream", config)

        if (self._write_back_client is None) or (write_concurrency is None):
            raise ValueError("Write back is only available if a database connection is provided.")

        write_result = self._write_back_client.write(
            G.name(),
            job_id,
            concurrency=write_concurrency if write_concurrency is not None else concurrency,
            property_overwrites=self._property_mapping(normalized_properties),
        )

        return NodePropertiesWriteResult(
            graphName=G.name(),
            nodeProperties=[p.effective_name() for p in normalized_properties],
            propertiesWritten=write_result.written_node_properties,
            writeMillis=write_result.write_millis,
            configuration=config,
        )

    def drop(
        self,
        G: Graph,
        node_properties: list[str],
        *,
        fail_if_missing: Optional[bool] = None,
        concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
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

    def _normalize_node_properties(self, node_properties: list[Union[str, NodePropertySpec]]) -> list[NodePropertySpec]:
        result = []
        for prop in node_properties:
            if isinstance(prop, str):
                result.append(NodePropertySpec(name=prop))
            elif isinstance(prop, NodePropertySpec):
                result.append(prop)
        return result

    def _property_mapping(self, normalized_properties: list[NodePropertySpec]) -> dict[str, str]:
        return {prop.name: prop.alias for prop in normalized_properties if prop.alias is not None}
