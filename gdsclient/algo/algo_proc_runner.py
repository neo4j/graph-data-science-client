from typing import Any, Dict

from ..graph.graph_object import Graph
from ..query_runner.query_runner import QueryResult, QueryRunner


class AlgoProcRunner:
    def __init__(self, query_runner: QueryRunner, proc_name: str):
        self._query_runner = query_runner
        self._proc_name = proc_name

    def _run_procedure(self, G: Graph, config: Dict[str, Any]) -> QueryResult:
        query = f"CALL {self._proc_name}($graph_name, $config)"

        params: Dict[str, Any] = {}
        params["graph_name"] = G.name()
        params["config"] = config

        return self._query_runner.run_query(query, params)

    def __call__(self, G: Graph, **config: Any) -> QueryResult:
        return self._run_procedure(G, config)

    def estimate(self, G: Graph, **config: Any) -> QueryResult:
        self._proc_name += "." + "estimate"
        return self._run_procedure(G, config)


# Path algorithm support
# for now: source and target node via previous cypher query / arbitrary id from 0..nodeCount 
        # supported types: Integer / (mostly also) Node object
# question: how does the user avoid the previous cypher query to get the node/node-id

# user wants to match:
#   single node
#   list of nodes (BFS)

# goal: convenience to match a source|target node(s)
# ideas:
#     some utility to avoid cypher queries
#           find specific node for label + properties parameters (make sure its only a single node)
#           arguments: list of labels, dictionary for parameters (how much do we want to rewrite the input into cypher?)
#     should not be an operation over a given graph object YET (in the future when using cypher on gds)
#     (a) could be a python wrapper specific function exposed the same as other gds functions
#       such as: gds.util.nodes(labelFilter:str, **nodeProperties:Any) !? how do we show this is not part of the library but python wrapper specific
#       one that expects only one result and another function that allows multiple (only needed by BFS for now)
#     (b) allow execution of an arbitrary cypher query and we make sure the result is only a single field and we transform the result (simple & cheap)
#           gds.findNode(cypherQuery:str) -> int | gds.findNodes(cypherQuery:str) -> list[int]
#           Example: 
#               gds.shortestPath.dijkstra.stream(
#                   graph, 
#                   sourceNode=gds.findNode("MATCH (source:Location {name: 'A'}) RETURN source"), 
#                   targetNode=gds.findNode("MATCH (target:Location {name: 'F'}) RETURN target"), 
#                   ...)