from gdsclient import GraphDataScience
from . import CollectingQueryRunner


def test_create_graph_native():
    runner = CollectingQueryRunner()
    gds = GraphDataScience(runner)
    graph = gds.graph.create("g", "A", "R")
    assert graph

    assert runner.queries == [
        "CALL gds.graph.create($graph_name, $node_projection, $relationship_projection)"
    ]
    assert runner.params == [
        {"graph_name": "g", "node_projection": "A", "relationship_projection": "R"}
    ]
