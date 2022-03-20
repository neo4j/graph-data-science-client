from typing import List

import pandas
from pandas.core.series import Series

from ..query_runner.query_runner import QueryRunner


class Graph:
    def __init__(self, name: str, query_runner: QueryRunner):
        self._name = name
        self._query_runner = query_runner

    def name(self) -> str:
        return self._name

    def _graph_info(self, yields: List[str] = []) -> Series:
        yield_suffix = "" if len(yields) == 0 else " YIELD " + ", ".join(yields)
        info = self._query_runner.run_query(
            f"CALL gds.graph.list($graph_name){yield_suffix}",
            {"graph_name": self._name},
        )

        if len(info) == 0:
            raise ValueError(f"There is no projected graph named '{self.name()}'")

        return info.squeeze()  # type: ignore

    def node_count(self) -> int:
        return self._graph_info(["nodeCount"])  # type: ignore

    def relationship_count(self) -> int:
        return self._graph_info(["relationshipCount"])  # type: ignore

    def node_properties(self, label: str) -> List[str]:
        labels_to_props = self._graph_info(["schema"])["nodes"]
        if label not in labels_to_props.keys():
            raise ValueError(f"There is no node label '{label}' projected onto '{self.name()}'")

        return list(labels_to_props[label].keys())

    def relationship_properties(self, type: str) -> List[str]:
        types_to_props = self._graph_info(["schema"])["relationships"]
        if type not in types_to_props.keys():
            raise ValueError(f"There is no relationship type '{type}' projected onto '{self.name()}'")

        return list(types_to_props[type].keys())

    def degree_distribution(self) -> Series:
        return pandas.Series(self._graph_info(["degreeDistribution"]))

    def density(self) -> float:
        return self._graph_info(["density"])  # type: ignore

    def memory_usage(self) -> str:
        return self._graph_info(["memoryUsage"])  # type: ignore

    def size_in_bytes(self) -> int:
        return self._graph_info(["sizeInBytes"])  # type: ignore

    def exists(self) -> bool:
        result = self._query_runner.run_query(
            "CALL gds.graph.exists($graph_name)",
            {"graph_name": self._name},
        )
        return result.squeeze()["exists"]  # type: ignore

    def drop(self) -> None:
        self._query_runner.run_query(
            "CALL gds.graph.drop($graph_name, false)",
            {"graph_name": self._name},
        )
