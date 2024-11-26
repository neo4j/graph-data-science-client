from __future__ import annotations

from types import TracebackType
from typing import Any, Optional, Type, Union

from pandas import Series

from ..call_parameters import CallParameters
from ..query_runner.query_runner import QueryRunner


class Graph:
    """
    A graph object that represents a graph in the graph catalog.
    It can be passed into algorithm endpoints to compute over the corresponding graph.
    It contains summary information about the graph.
    """

    def __init__(self, name: str, query_runner: QueryRunner):
        self._name = name
        self._query_runner = query_runner
        self._db = query_runner.database()

    def __enter__(self: Graph) -> Graph:
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

    def _graph_info(self, yields: list[str] = []) -> Series[Any]:
        yield_db = "database" in yields
        yields_with_db = yields if yield_db else yields + ["database"]

        info = self._query_runner.call_procedure(
            endpoint="gds.graph.list",
            params=CallParameters(graph_name=self._name),
            yields=yields_with_db,
            custom_error=False,
        )

        if len(info) == 0:
            raise ValueError(f"There is no projected graph named '{self.name()}'")
        if len(info) > 1:
            # for multiple dbs we can have the same graph name. But db + graph name is unique
            info = info[info["database"] == self._db]

        if not yield_db:
            info = info.drop(columns=["database"])

        return info.squeeze()  # type: ignore

    def database(self) -> str:
        """
        Returns:
            the name of the database the graph is stored in
        """
        return self._graph_info(["database"])  # type: ignore

    def configuration(self) -> "Series[Any]":
        """
        Returns:
            the configuration of the graph
        """
        return Series(self._graph_info(["configuration"]))

    def node_count(self) -> int:
        """
        Returns:
            the number of nodes in the graph

        """
        return self._graph_info(["nodeCount"])  # type: ignore

    def relationship_count(self) -> int:
        """
        Returns:
            the number of relationships in the graph
        """
        return self._graph_info(["relationshipCount"])  # type: ignore

    def node_labels(self) -> list[str]:
        """
        Returns:
            the node labels in the graph
        """
        return list(self._graph_info(["schema"])["nodes"].keys())

    def relationship_types(self) -> list[str]:
        """
        Returns:
            the relationship types in the graph
        """
        return list(self._graph_info(["schema"])["relationships"].keys())

    def node_properties(self, label: Optional[str] = None) -> Union[Series[str], list[str]]:
        """
        Args:
            label: the node label to get the properties for

        Returns:
            the node properties for the given label

        """
        labels_to_props = self._graph_info(["schema"])["nodes"]

        if not label:
            return Series({key: list(val.keys()) for key, val in labels_to_props.items()})

        if label not in labels_to_props.keys():
            raise ValueError(f"There is no node label '{label}' projected onto '{self.name()}'")

        return list(labels_to_props[label].keys())

    def relationship_properties(self, type: Optional[str] = None) -> Union[Series[str], list[str]]:
        """
        Args:
            type: the relationship type to get the properties for

        Returns:
            the relationship properties for the given type
        """
        types_to_props = self._graph_info(["schema"])["relationships"]

        if not type:
            return Series({key: list(val.keys()) for key, val in types_to_props.items()})

        if type not in types_to_props.keys():
            raise ValueError(f"There is no relationship type '{type}' projected onto '{self.name()}'")

        return list(types_to_props[type].keys())

    def degree_distribution(self) -> "Series[float]":
        """
        Returns:
            the degree distribution of the graph
        """
        return Series(self._graph_info(["degreeDistribution"]))

    def density(self) -> float:
        """
        Returns:
            the density of the graph
        """
        return self._graph_info(["density"])  # type: ignore

    def memory_usage(self) -> str:
        """
        Returns:
            the memory usage of the graph
        """
        return self._graph_info(["memoryUsage"])  # type: ignore

    def size_in_bytes(self) -> int:
        """
        Returns:
            the size of the graph in bytes
        """
        return self._graph_info(["sizeInBytes"])  # type: ignore

    def exists(self) -> bool:
        """
        Returns:
            whether the graph exists
        """
        result = self._query_runner.call_procedure(
            endpoint="gds.graph.exists",
            params=CallParameters(graph_name=self._name),
            custom_error=False,
        )
        return result.squeeze()["exists"]  # type: ignore

    def drop(self, failIfMissing: bool = False) -> "Series[str]":
        """
        Args:
            failIfMissing: whether to fail if the graph does not exist

        Returns:
            the result of the drop operation

        """
        result = self._query_runner.call_procedure(
            endpoint="gds.graph.drop",
            params=CallParameters(graph_name=self._name, failIfMissing=failIfMissing),
            custom_error=False,
        )

        return result.squeeze()  # type: ignore

    def creation_time(self) -> Any:  # neo4j.time.DateTime not exported
        """
        Returns:
            the creation time of the graph

        """
        return self._graph_info(["creationTime"])

    def modification_time(self) -> Any:  # neo4j.time.DateTime not exported
        """
        Returns:
            the modification time of the graph
        """
        return self._graph_info(["modificationTime"])

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}(name={self.name()}, "
            f"node_count={self.node_count()}, relationship_count={self.relationship_count()})"
        )

    def __repr__(self) -> str:
        yield_fields = [
            "graphName",
            "nodeCount",
            "relationshipCount",
            "database",
            "configuration",
            "schema",
            "memoryUsage",
        ]
        return f"{self.__class__.__name__}({self._graph_info(yields=yield_fields).to_dict()})"
