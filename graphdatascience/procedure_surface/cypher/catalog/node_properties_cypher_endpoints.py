from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience import Graph, QueryRunner
from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.node_properties_endpoints import (
    NodePropertiesDropResult,
    NodePropertiesEndpoints,
    NodePropertiesWriteResult,
    NodePropertySpec,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.procedure_surface.utils.result_utils import transpose_property_columns
from graphdatascience.query_runner.gds_arrow_client import GdsArrowClient


class NodePropertiesCypherEndpoints(NodePropertiesEndpoints):
    def __init__(self, query_runner: QueryRunner, gds_arrow_client: Optional[GdsArrowClient] = None):
        self._query_runner = query_runner
        self._gds_arrow_client = gds_arrow_client

    def stream(
        self,
        G: Graph,
        node_properties: Union[str, List[str]],
        *,
        list_node_labels: Optional[bool] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,  # setting the node id is not supported by the Cypher procedure
    ) -> DataFrame:
        if self._gds_arrow_client is not None:
            database = self._query_runner.database()
            if database is None:
                raise ValueError("The database is not set")

            return self._gds_arrow_client.get_node_properties(
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

            result = self._query_runner.call_procedure(endpoint="gds.graph.nodeProperties.stream", params=params)

            return transpose_property_columns(result, list_node_labels or False)

    def write(
        self,
        G: Graph,
        node_properties: NodePropertySpec,
        *,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,
    ) -> NodePropertiesWriteResult:
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
            node_properties=node_properties.to_dict(),
            node_labels=node_labels if node_labels is not None else ["*"],
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.graph.nodeProperties.write", params=params).squeeze()

        return NodePropertiesWriteResult(**result.to_dict())

    def drop(
        self,
        G: Graph,
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
