from __future__ import annotations

import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, List, Optional, Union

from pydantic import Field, field_validator

from graphdatascience import Graph
from graphdatascience.procedure_surface.utils.GdsBaseModel import GdsBaseModel


class CatalogEndpoints(ABC):
    @abstractmethod
    def list(self, G: Optional[Union[Graph, str]] = None) -> List[GraphListResult]:
        """List graphs in the graph catalog.

        Args:
            G (Optional[Union[Graph, str]], optional): Graph object or name to filter results.
               If None, list all graphs. Defaults to None.

        Returns:
            List[GraphListResult]: List of graph metadata objects containing information like
                                 graph name, node count, relationship count, etc.
        """
        pass

    @abstractmethod
    def drop(self, G: Union[Graph, str], fail_if_missing: Optional[bool] = None) -> GraphListResult:
        """Drop a graph from the graph catalog.

        Args:
            G (Union[Graph, str]): Graph object or name to drop.
            fail_if_missing (Optional[bool], optional): Whether to fail if the graph is missing. Defaults to None.

        Returns:
              GraphListResult: Graph metadata object containing information like
                               graph name, node count, relationship count, etc.
        """

    @abstractmethod
    def filter(
        self,
        G: Graph,
        graph_name: str,
        node_filter: str,
        relationship_filter: str,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
    ) -> GraphFilterResult:
        """Create a subgraph of a graph based on a filter expression.

        Args:
            G (Graph): Graph object to filter on
            graph_name (str): Name of subgraph to create
            node_filter (str): Filter expression for nodes
            relationship_filter (str): Filter expression for relationships
            concurrency (Optional[int], optional): Number of concurrent threads to use. Defaults to None.
            job_id (Optional[str], optional): Unique identifier for the filtering job. Defaults to None.

        Returns:
            GraphFilterResult: Filter result containing information like
                                graph name, node count, relationship count, etc.
        """
        pass


class GraphListResult(GdsBaseModel):
    graph_name: str
    database: str
    database_location: str
    configuration: dict[str, Any]
    memory_usage: str
    size_in_bytes: int
    node_count: int
    relationship_count: int
    creation_time: datetime
    modification_time: datetime
    graph_schema: dict[str, Any] = Field(alias="schema")
    schema_with_orientation: dict[str, Any]
    degree_distribution: Optional[dict[str, Any]] = None

    @field_validator("creation_time", "modification_time", mode="before")
    @classmethod
    def strip_timezone(cls, value: Any) -> Any:
        if isinstance(value, str):
            return re.sub(r"\[.*\]$", "", value)
        return value


class GraphFilterResult(GdsBaseModel):
    graph_name: str
    from_graph_name: str
    node_filter: str
    relationship_filter: str
    node_count: int
    relationship_count: int
    project_millis: int
