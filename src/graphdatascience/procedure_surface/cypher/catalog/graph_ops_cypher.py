from __future__ import annotations

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.graph_info import GraphInfo, GraphInfoWithDegrees
from graphdatascience.procedure_surface.cypher.catalog.utils import (
    GRAPH_INFO_WITH_DEGREES_YIELDS,
    GRAPH_INFO_YIELDS,
)
from graphdatascience.query_runner.query_mode import QueryMode
from graphdatascience.query_runner.query_runner import QueryRunner


class GraphOpsCypher:
    def __init__(self, query_runner: QueryRunner) -> None:
        self._query_runner = query_runner

    def list(self, graph_name: str) -> list[GraphInfoWithDegrees]:
        result = self._query_runner.call_procedure(
            endpoint="gds.graph.list",
            params=CallParameters(graph_name=graph_name),
            yields=GRAPH_INFO_WITH_DEGREES_YIELDS,
            custom_error=False,
        )
        return [GraphInfoWithDegrees(**row) for _, row in result.iterrows()]

    def exists(self, graph_name: str) -> bool:
        result = self._query_runner.call_procedure(
            endpoint="gds.graph.exists",
            params=CallParameters(graph_name=graph_name),
            custom_error=False,
        )
        return bool(result.iloc[0]["exists"])

    def drop(self, graph_name: str, fail_if_missing: bool = True, username: str | None = None) -> GraphInfo | None:
        params = CallParameters(graph_name=graph_name, failIfMissing=fail_if_missing)

        if username is not None:
            # positional params. order has to be preserved
            params["dbName"] = ""
            params["username"] = username

        info = self._query_runner.call_procedure(
            endpoint="gds.graph.drop",
            params=params,
            yields=GRAPH_INFO_YIELDS,
            custom_error=False,
            # dropping is idempotent as long as a missing graph is not an error
            retryable=not fail_if_missing,
            mode=QueryMode.WRITE,
        )

        if info.empty:
            return None

        return GraphInfo(**info.iloc[0])
