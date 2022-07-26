from typing import Any, Dict

from pandas.core.series import Series

from ..caller_base import CallerBase
from ..error.illegal_attr_checker import IllegalAttrChecker
from .graph_object import Graph


class GraphExportCsvRunner(CallerBase, IllegalAttrChecker):
    # TODO: Add an integration test for this call.
    def __call__(self, G: Graph, **config: Any) -> Series:
        return self._export_call(G, config)

    def _export_call(self, G: Graph, config: Dict[str, Any]) -> Series:
        query = f"CALL {self._namespace}($graph_name, $config)"
        params = {"graph_name": G.name(), "config": config}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def estimate(self, G: Graph, **config: Any) -> Series:
        self._namespace += ".estimate"

        return self._export_call(G, config)


class GraphExportRunner(CallerBase, IllegalAttrChecker):
    def __call__(self, G: Graph, **config: Any) -> Series:
        return self._export_call(G, config)

    def _export_call(self, G: Graph, config: Dict[str, Any]) -> Series:
        query = f"CALL {self._namespace}($graph_name, $config)"
        params = {"graph_name": G.name(), "config": config}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    @property
    def csv(self) -> GraphExportCsvRunner:
        self._namespace += ".csv"

        return GraphExportCsvRunner(self._query_runner, self._namespace, self._server_version)
