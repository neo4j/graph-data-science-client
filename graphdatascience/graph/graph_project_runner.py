from typing import Any, Tuple

from pandas import Series

from ..error.illegal_attr_checker import IllegalAttrChecker
from .graph_object import Graph
from .graph_type_check import from_graph_type_check


class GraphProjectRunner(IllegalAttrChecker):
    def __call__(
        self, graph_name: str, node_spec: Any, relationship_spec: Any, **config: Any
    ) -> Tuple[Graph, "Series[Any]"]:
        result = self._query_runner.run_query_with_logging(
            f"CALL {self._namespace}($graph_name, $node_spec, $relationship_spec, $config)",
            {
                "graph_name": graph_name,
                "node_spec": node_spec,
                "relationship_spec": relationship_spec,
                "config": config,
            },
        ).squeeze()

        return Graph(graph_name, self._query_runner, self._server_version), result

    def estimate(self, node_projection: Any, relationship_projection: Any, **config: Any) -> "Series[Any]":
        """
        Estimate the memory required to project a graph using a native projection.

        Args:
            node_projection: the node projection to use.
            relationship_projection: the relationship projection to use.
            **config: configuration for the projection.

        Returns:
            A pandas Series containing the estimated memory requirements.
        """
        self._namespace += ".estimate"
        result = self._query_runner.run_query(
            f"CALL {self._namespace}($node_spec, $relationship_spec, $config)",
            {
                "node_spec": node_projection,
                "relationship_spec": relationship_projection,
                "config": config,
            },
        )

        return result.squeeze()  # type: ignore

    @property
    def cypher(self) -> "GraphProjectRunner":
        """
        cypher(graph_name: str, node_query: Any, relationship_query: Any, **config: Any) -> Tuple[Graph, "Series[Any]"]

        Projects a new graph to the graph catalog using a Cypher projection.

        Args:
            graph_name: the name to give the projected graph.
            node_query: the node query.
            relationship_query: the relationship query.
            config: the configuration for the projection.

        Returns:
            A tuple containing a graph object representing the projected graph
            and a Series containing metadata about the projection.
        """
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
    ) -> Tuple[Graph, "Series[Any]"]:
        """
        Create a subgraph from a given graph.

        Args:
            graph_name: The name of the new graph.
            from_G: The graph to create the subgraph from.
            node_filter: A Cypher predicate to filter nodes.
            relationship_filter: A Cypher predicate to filter relationships.
            **config: Additional configuration parameters.

        Returns:
            The new graph and results of executing the query.

        """
        self._namespace += ".subgraph"
        result = self._query_runner.run_query_with_logging(
            f"CALL {self._namespace}($graph_name, $from_graph_name, $node_filter, $relationship_filter, $config)",
            {
                "graph_name": graph_name,
                "from_graph_name": from_G.name(),
                "node_filter": node_filter,
                "relationship_filter": relationship_filter,
                "config": config,
            },
        ).squeeze()

        return Graph(graph_name, self._query_runner, self._server_version), result
