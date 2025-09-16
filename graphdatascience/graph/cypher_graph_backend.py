from __future__ import annotations

from typing import Any

import neo4j

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.graph_backend import GraphBackend, GraphInfo


class CypherGraphBackend(GraphBackend):
    def __init__(self, name: str, query_runner: neo4j.QueryRunner) -> None:
        self._name = name
        self._query_runner = query_runner
        self._db = self._query_runner.database()

    def graph_info(self) -> GraphInfo:
        info = self._query_runner.call_procedure(
            endpoint="gds.graph.list",
            params=CallParameters(graph_name=self._name),
            custom_error=False,
        )

        if len(info) == 0:
            raise ValueError(f"There is no projected graph named '{self._name}'")
        if len(info) > 1:
            # for multiple dbs we can have the same graph name. But db + graph name is unique
            info = info[info["database"] == self._db]

        return GraphInfo(**info.squeeze())

    def exists(self) -> bool:
        result = self._query_runner.call_procedure(
            endpoint="gds.graph.exists",
            params=CallParameters(graph_name=self._name),
            custom_error=False,
        )
        return result.squeeze()["exists"]  # type: ignore

    def drop(self, fail_if_missing: bool = True) -> dict[str, Any]:
        return (
            self._query_runner.call_procedure(
                endpoint="gds.graph.drop",
                params=CallParameters(graph_name=self._name, failIfMissing=fail_if_missing),
                custom_error=False,
            )
            .squeeze()
            .to_dict()
        )  # type: ignore
