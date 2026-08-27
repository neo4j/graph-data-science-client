from __future__ import annotations

from pandas import DataFrame

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.catalog.node_properties_endpoints import NodePropertiesEndpoints
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS


class NodePropertyEndpoints:
    """Endpoints for streaming a single node property"""

    def __init__(self, node_properties_endpoints: NodePropertiesEndpoints) -> None:
        self._node_properties_endpoints = node_properties_endpoints

    def stream(
        self,
        G: Graph,
        node_property: str,
        *,
        list_node_labels: bool | None = False,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        db_node_properties: list[str] | None = None,
    ) -> DataFrame:
        """
        Streams the specified node property from the graph.

        Parameters
        ----------
        G
           Graph object to use
        node_property
            The node property to stream
        list_node_labels
            Whether to include node labels in the stream
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        concurrency
            Number of concurrent threads to use.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        db_node_properties
            Retrieves additional node properties from the database and attaches them to the stream.
        Returns
        -------
        pandas.DataFrame
            The streamed node property as `nodeId` and `propertyValue` columns, plus a `nodeLabels` column
            if `list_node_labels` is set and one column per requested `db_node_properties`.
        """
        result = self._node_properties_endpoints.stream(
            G,
            node_property,
            list_node_labels=list_node_labels,
            node_labels=node_labels,
            concurrency=concurrency,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            db_node_properties=db_node_properties,
        )

        return result.rename(columns={node_property: "propertyValue"})
