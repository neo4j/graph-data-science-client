from gdsclient import GraphDataScience
from . import CollectingQueryRunner

RUNNER = CollectingQueryRunner()
gds = GraphDataScience(RUNNER)


def test_create_graph_native():
    graph = gds.graph.create("g", "A", "R")
    assert graph

    assert (
        RUNNER.last_query()
        == "CALL gds.graph.create($graph_name, $node_spec, $relationship_spec)"
    )
    assert RUNNER.last_params() == {
        "graph_name": "g",
        "node_spec": "A",
        "relationship_spec": "R",
    }


def test_create_graph_native_estimate():
    gds.graph.create.estimate("A", "R")

    assert (
        RUNNER.last_query()
        == "CALL gds.graph.create.estimate($node_spec, $relationship_spec)"
    )
    assert RUNNER.last_params() == {
        "node_spec": "A",
        "relationship_spec": "R",
    }


def test_create_graph_cypher():
    graph = gds.graph.create.cypher(
        "g", "RETURN 0 as id", "RETURN 0 as source, 0 as target"
    )
    assert graph

    assert (
        RUNNER.last_query()
        == "CALL gds.graph.create.cypher($graph_name, $node_spec, $relationship_spec)"
    )
    assert RUNNER.last_params() == {
        "graph_name": "g",
        "node_spec": "RETURN 0 as id",
        "relationship_spec": "RETURN 0 as source, 0 as target",
    }


def test_create_graph_cypher_estimate():
    gds.graph.create.cypher.estimate(
        "RETURN 0 as id", "RETURN 0 as source, 0 as target"
    )

    assert (
        RUNNER.last_query()
        == "CALL gds.graph.create.cypher.estimate($node_spec, $relationship_spec)"
    )
    assert RUNNER.last_params() == {
        "node_spec": "RETURN 0 as id",
        "relationship_spec": "RETURN 0 as source, 0 as target",
    }
