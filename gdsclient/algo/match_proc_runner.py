from typing import Any, Collection, Dict

from ..graph.graph_object import Graph
from ..query_runner.query_runner import QueryResult, QueryRunner


class MatchProcRunner:
    NodeMatch = Dict[str, Collection[str]]

    def __init__(self, query_runner: QueryRunner, proc_name: str):
        self._query_runner = query_runner
        self._proc_name = proc_name

    def estimate(self, G: Graph, **config: Any) -> QueryResult:
        self._proc_name += "." + "estimate"
        return self._run_procedure(G, config)

    def __call__(self, G: Graph, **config: Any) -> QueryResult:
        return self._run_procedure(G, config)

    def _match_node(self, node_match: NodeMatch) -> QueryResult:
        label_match = None
        prop_match = None

        if "labels" in node_match and len(node_match["labels"]) > 0:
            label_match = " AND ".join([f"n:{label}" for label in node_match["labels"]])
        if "properties" in node_match and len(node_match["properties"]) > 0:
            assert isinstance(node_match["properties"], Dict)  # Make type checker happy
            prop_match = " AND ".join(
                [f"n.{key} = '{val}'" for key, val in node_match["properties"].items()]
            )

        if label_match and prop_match:
            query = f"MATCH (n) WHERE {label_match} AND {prop_match} RETURN id(n) AS id"
        elif label_match:
            query = f"MATCH (n) WHERE {label_match} RETURN id(n) AS id"
        elif prop_match:
            query = f"MATCH (n) WHERE {prop_match} RETURN id(n) AS id"
        else:
            query = "MATCH (n) RETURN id(n) AS id"

        return self._query_runner.run_query(query)

    def _run_procedure(
        self,
        G: Graph,
        config: Dict[str, Any],
    ) -> QueryResult:
        for node_arg in ["sourceNode", "targetNode", "startNode", "startNodeId"]:
            if node_arg not in config:
                continue

            node_match = self._match_node(config[node_arg])
            if len(node_match) != 1:
                raise ValueError(
                    f"{node_arg} did not match with exactly one node: {node_match}"
                )
            config[node_arg] = node_match[0]["id"]

        params: Dict[str, Any] = {}
        params["graph_name"] = G.name()
        params["config"] = config

        query = f"CALL {self._proc_name}($graph_name, $config)"

        return self._query_runner.run_query(query, params)
