from gdsclient import GraphDataScience

from . import CollectingQueryRunner

runner = CollectingQueryRunner()
gds = GraphDataScience(runner)


def test_project_graph_native():
    graph = gds.graph.project("g", "A", "R")
    assert graph.name == "g"

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
    assert graph.name == "g"

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


def test_project_subgraph():
    from_graph = gds.graph.project("g", "*", "*")
    gds.beta.graph.project.subgraph(
        "s", from_graph, {"Node": {}}, {"REL": {}}, concurrency=2
    )

    assert (
        runner.last_query()
        == "CALL "
        + "gds.beta.graph.project.subgraph($graph_name, $from_graph_name, $node_filter, $relationship_filter, $config)"
    )
    assert runner.last_params() == {
        "graph_name": "s",
        "from_graph_name": "g",
        "node_filter": {"Node": {}},
        "relationship_filter": {"REL": {}},
        "config": {"concurrency": 2},
    }


def test_graph_list():
    gds.graph.list()

    assert runner.last_query() == "CALL gds.graph.list()"
    assert runner.last_params() == {}

    graph = gds.graph.project("g", "A", "R")

    gds.graph.list(graph)

    assert runner.last_query() == "CALL gds.graph.list($graph_name)"
    assert runner.last_params() == {"graph_name": graph.name}
