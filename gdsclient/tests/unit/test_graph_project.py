from gdsclient import GraphDataScience

from . import CollectingQueryRunner

runner = CollectingQueryRunner()
gds = GraphDataScience(runner)


def test_project_graph_native():
    graph = gds.graph.project("g", "A", "R")
    assert graph

    assert (
        runner.last_query()
        == "CALL gds.graph.project($graph_name, $node_spec, $relationship_spec)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "node_spec": "A",
        "relationship_spec": "R",
    }


def test_project_graph_native_estimate():
    gds.graph.project.estimate("A", "R")

    assert (
        runner.last_query()
        == "CALL gds.graph.project.estimate($node_spec, $relationship_spec)"
    )
    assert runner.last_params() == {
        "node_spec": "A",
        "relationship_spec": "R",
    }


def test_project_graph_cypher():
    graph = gds.graph.project.cypher(
        "g", "RETURN 0 as id", "RETURN 0 as source, 0 as target"
    )
    assert graph

    assert (
        runner.last_query()
        == "CALL gds.graph.project.cypher($graph_name, $node_spec, $relationship_spec)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "node_spec": "RETURN 0 as id",
        "relationship_spec": "RETURN 0 as source, 0 as target",
    }


def test_project_graph_cypher_estimate():
    gds.graph.project.cypher.estimate(
        "RETURN 0 as id", "RETURN 0 as source, 0 as target"
    )

    assert (
        runner.last_query()
        == "CALL gds.graph.project.cypher.estimate($node_spec, $relationship_spec)"
    )
    assert runner.last_params() == {
        "node_spec": "RETURN 0 as id",
        "relationship_spec": "RETURN 0 as source, 0 as target",
    }
