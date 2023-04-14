from typing import Any, Dict

from pandas import Series

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from .graph_object import Graph
from .graph_type_check import graph_type_check


class GraphExportCsvRunner(IllegalAttrChecker):
    # TODO: Add an integration test for this call.
    def __call__(self, G: Graph, **config: Any) -> "Series[Any]":
        """
        Export a graph to CSV.

        Args:
            G: Graph to export.
            **config: Additional configuration parameters.

        Returns:
            Size of the CSV export.

        """
        return self._export_call(G, config)

    @graph_type_check
    def _export_call(self, G: Graph, config: Dict[str, Any]) -> "Series[Any]":
        query = f"CALL {self._namespace}($graph_name, $config)"
        params = {"graph_name": G.name(), "config": config}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    @graph_type_check
    def estimate(self, G: Graph, **config: Any) -> "Series[Any]":
        """
        Estimate the size of the CSV export.

        Args:
            G: Graph to export.
            **config: Additional configuration parameters.

        Returns:
            Estimated size of the CSV export.

        """
        self._namespace += ".estimate"

        return self._export_call(G, config)


class GraphExportCsvEndpoints(UncallableNamespace, IllegalAttrChecker):
    @property
    def csv(self) -> GraphExportCsvRunner:
        """
        csv(self, G: Graph, **config: Any) -> "Series[Any]":
        Export a graph to CSV.

        Args:
            G: Graph to export.
            **config: Additional configuration parameters.

        Returns:
            Size of the CSV export.

        """
        self._namespace += ".csv"

        return GraphExportCsvRunner(self._query_runner, self._namespace, self._server_version)


class GraphExportRunner(IllegalAttrChecker):
    def __call__(self, G: Graph, **config: Any) -> "Series[Any]":
        return self._export_call(G, config)

    @graph_type_check
    def _export_call(self, G: Graph, config: Dict[str, Any]) -> "Series[Any]":
        query = f"CALL {self._namespace}($graph_name, $config)"
        params = {"graph_name": G.name(), "config": config}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    @property
    def csv(self) -> GraphExportCsvRunner:
        self._namespace += ".csv"

        return GraphExportCsvRunner(self._query_runner, self._namespace, self._server_version)
