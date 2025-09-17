from __future__ import annotations

from abc import ABC
from datetime import datetime
from types import TracebackType
from typing import Any, Optional, Type

from graphdatascience.procedure_surface.api.catalog.graph_backend import GraphBackend
from graphdatascience.procedure_surface.api.catalog.graph_info import GraphInfo


class GraphV2(ABC):
    """
    A graph object that represents a graph in the graph catalog.
    It can be passed into algorithm endpoints to compute over the corresponding graph.
    It contains summary information about the graph.
    """

    def __init__(self, name: str, backend: GraphBackend):
        self._name = name
        self._backend = backend

    def __enter__(self: GraphV2) -> GraphV2:
        return self

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.drop()

    def name(self) -> str:
        """
        Returns:
            the name of the graph
        """
        return self._name

    def configuration(self) -> dict[str, Any]:
        """
        Returns:
            the configuration of the graph
        """
        return self._backend.graph_info().configuration

    def node_count(self) -> int:
        """
        Returns:
            the number of nodes in the graph

        """
        return self._backend.graph_info().node_count

    def relationship_count(self) -> int:
        """
        Returns:
            the number of relationships in the graph
        """
        return self._backend.graph_info().relationship_count

    def node_labels(self) -> list[str]:
        """
        Returns:
            the node labels in the graph
        """
        return list(self._backend.graph_info().graph_schema["nodes"].keys())

    def relationship_types(self) -> list[str]:
        """
        Returns:
            the relationship types in the graph
        """
        return list(self._backend.graph_info().graph_schema["relationships"].keys())

    def node_properties(self) -> dict[str, list[str]]:
        """
        Returns:
            the node properties per node label

        """
        labels_to_props = self._backend.graph_info().graph_schema["nodes"]

        return {key: list(val.keys()) for key, val in labels_to_props.items()}

    def relationship_properties(self) -> dict[str, list[str]]:
        """
        Returns:
            the relationship properties per relationship type
        """
        types_to_props = self._backend.graph_info().graph_schema["relationships"]

        return {key: list(val.keys()) for key, val in types_to_props.items()}

    def degree_distribution(self) -> dict[str, float | int]:
        """
        Returns:
            the degree distribution of the graph
        """
        return self._backend.graph_info().degree_distribution

    def density(self) -> float:
        """
        Returns:
            the density of the graph
        """
        return self._backend.graph_info().density

    def memory_usage(self) -> str:
        """
        Returns:
            the memory usage of the graph
        """
        return self._backend.graph_info().memory_usage

    def size_in_bytes(self) -> int:
        """
        Returns:
            the size of the graph in bytes
        """
        return self._backend.graph_info().size_in_bytes

    def exists(self) -> bool:
        """
        Returns:
            whether the graph exists
        """
        return self._backend.exists()

    def drop(self, failIfMissing: bool = False) -> Optional[GraphInfo]:
        """
        Args:
            failIfMissing: whether to fail if the graph does not exist

        Returns:
            the result of the drop operation

        """
        return self._backend.drop(fail_if_missing=failIfMissing)

    def creation_time(self) -> datetime:
        """
        Returns:
            the creation time of the graph

        """
        return self._backend.graph_info().creation_time

    def modification_time(self) -> datetime:
        """
        Returns:
            the modification time of the graph
        """
        return self._backend.graph_info().modification_time

    def __str__(self) -> str:
        info = self._backend.graph_info()
        return (
            f"{self.__class__.__name__}(name={self.name()}, "
            f"node_count={info.node_count}, relationship_count={info.relationship_count})"
        )

    def __repr__(self) -> str:
        fields = {
            "graph_name",
            "node_count",
            "relationship_count",
            "database",
            "configuration",
            "schema",
            "memory_usage",
        }
        return f"{self.__class__.__name__}({self._backend.graph_info().model_dump(include=fields)})"
