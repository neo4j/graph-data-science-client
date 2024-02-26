from __future__ import annotations

from typing import Any

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..server_version.compatible_with import compatible_with
from .graph_object import Graph
from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.graph_create_result import GraphCreateResult
from graphdatascience.server_version.server_version import ServerVersion


class GraphProjectRemoteRunner(IllegalAttrChecker):
    _SCHEMA_KEYS = ["nodePropertySchema", "relationshipPropertySchema"]

    @compatible_with("project", min_inclusive=ServerVersion(2, 6, 0))
    def __call__(self, graph_name: str, query: str, **config: Any) -> GraphCreateResult:
        placeholder = "<>"  # host and token will be added by query runner
        self.map_property_types(config)
        params = CallParameters(
            graph_name=graph_name,
            query=query,
            token=placeholder,
            host=placeholder,
            remote_database=self._query_runner.database(),
            config=config,
        )
        result = self._query_runner.call_procedure(
            endpoint=self._namespace,
            params=params,
        ).squeeze()
        return GraphCreateResult(Graph(graph_name, self._query_runner, self._server_version), result)

    @staticmethod
    def map_property_types(config: dict[str, Any]) -> None:
        for key in GraphProjectRemoteRunner._SCHEMA_KEYS:
            if key in config:
                config[key] = {k: v.value for k, v in config[key].items()}
