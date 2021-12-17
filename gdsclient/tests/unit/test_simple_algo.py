from gdsclient import GraphDataScience
from . import TestQueryRunner


RUNNER = TestQueryRunner()
gds = GraphDataScience(RUNNER)
GRAPH_NAME = "g"


def test_algoName_mutate():
    graph = gds.graph.create(GRAPH_NAME, "Node", "REL")

    gds.algoName.mutate(graph, mutateProperty="rank", dampingFactor=0.2, tolerance=0.3)

    assert RUNNER.last_query() == "CALL gds.algoName.mutate($graph_name, $config)"
    assert RUNNER.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"mutateProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_stats():
    graph = gds.graph.create(GRAPH_NAME, "Node", "REL")

    gds.algoName.stats(graph, dampingFactor=0.2, tolerance=0.3)

    assert RUNNER.last_query() == "CALL gds.algoName.stats($graph_name, $config)"
    assert RUNNER.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_stream():
    graph = gds.graph.create(GRAPH_NAME, "Node", "REL")

    gds.algoName.stream(graph, dampingFactor=0.2, tolerance=0.3)

    assert RUNNER.last_query() == "CALL gds.algoName.stream($graph_name, $config)"
    assert RUNNER.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_write():
    graph = gds.graph.create(GRAPH_NAME, "Node", "REL")

    gds.algoName.write(graph, writeProperty="rank", dampingFactor=0.2, tolerance=0.3)

    assert RUNNER.last_query() == "CALL gds.algoName.write($graph_name, $config)"
    assert RUNNER.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"writeProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3},
    }
