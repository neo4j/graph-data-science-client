from gdsclient import GraphDataScience
from . import CollectingQueryRunner

RUNNER = CollectingQueryRunner()
gds = GraphDataScience(RUNNER)


def test_create_graph_native():
    graph = gds.graph.create("g", "A", "R")
    assert graph

    assert (
        RUNNER.last_query()
        == "CALL gds.graph.create($graph_name, $node_projection, $relationship_projection)"
    )
    assert RUNNER.last_params() == {
        "graph_name": "g",
        "node_projection": "A",
        "relationship_projection": "R",
    }


def test_create_graph_native_estimate():
    graph = gds.graph.create.estimate("A", "R")
    assert graph

    assert (
        RUNNER.last_query()
        == "CALL gds.graph.create.estimate($node_projection, $relationship_projection)"
    )
    assert RUNNER.last_params() == {
        "node_projection": "A",
        "relationship_projection": "R",
    }
