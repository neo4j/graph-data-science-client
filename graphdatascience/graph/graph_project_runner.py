from __future__ import annotations

from typing import Any

from pandas import Series

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..session.schema import NODE_PROPERTY_SCHEMA, RELATIONSHIP_PROPERTY_SCHEMA
from .graph_object import Graph
from .graph_type_check import from_graph_type_check
from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.graph_create_result import GraphCreateResult
from graphdatascience.server_version.compatible_with import compatible_with
from graphdatascience.server_version.server_version import ServerVersion


class GraphProjectRunner(IllegalAttrChecker):
    def __call__(self, graph_name: str, node_spec: Any, relationship_spec: Any, **config: Any) -> GraphCreateResult:
        params = CallParameters(
            graph_name=graph_name,
            node_spec=node_spec,
            relationship_spec=relationship_spec,
            config=config,
        )
        result = self._query_runner.call_procedure(
            endpoint=self._namespace,
            params=params,
            logging=True,
        ).squeeze()

        return GraphCreateResult(Graph(graph_name, self._query_runner, self._server_version), result)

    def estimate(self, node_projection: Any, relationship_projection: Any, **config: Any) -> "Series[Any]":
        self._namespace += ".estimate"
        result = self._query_runner.call_procedure(
            endpoint=self._namespace,
            params=CallParameters(
                node_spec=node_projection,
                relationship_spec=relationship_projection,
                config=config,
            ),
        )

        return result.squeeze()  # type: ignore

    @property
    def cypher(self) -> GraphProjectRunner:
        return GraphProjectRunner(self._query_runner, self._namespace + ".cypher", self._server_version)


class GraphProjectBetaRunner(IllegalAttrChecker):
    @from_graph_type_check
    def subgraph(
        self,
        graph_name: str,
        from_G: Graph,
        node_filter: str,
        relationship_filter: str,
        **config: Any,
    ) -> GraphCreateResult:
        self._namespace += ".subgraph"
        params = CallParameters(
            graph_name=graph_name,
            from_graph_name=from_G.name(),
            node_filter=node_filter,
            relationship_filter=relationship_filter,
            config=config,
        )
        result = self._query_runner.call_procedure(
            endpoint=self._namespace,
            params=params,
            logging=True,
        ).squeeze()

        return GraphCreateResult(Graph(graph_name, self._query_runner, self._server_version), result)


class GraphProjectRemoteRunner(IllegalAttrChecker):
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
        for key in [NODE_PROPERTY_SCHEMA, RELATIONSHIP_PROPERTY_SCHEMA]:
            if key in config:
                config[key] = {k: v.value for k, v in config[key].items()}
