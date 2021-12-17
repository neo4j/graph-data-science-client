from gdsclient import GraphDataScience
from . import TestQueryRunner


RUNNER = TestQueryRunner()
gds = GraphDataScience(RUNNER)
GRAPH_NAME = "g"


def test_pageRank_mutate():
    graph = gds.graph.create(GRAPH_NAME, "Node", "REL")

    gds.pageRank.mutate(graph, mutateProperty="rank", dampingFactor=0.2, tolerance=0.3)

    assert RUNNER.last_query() == "CALL gds.pageRank.mutate($graph_name, $config)"
    assert RUNNER.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"mutateProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_pageRank_stats():
    graph = gds.graph.create(GRAPH_NAME, "Node", "REL")

    gds.pageRank.stats(graph, dampingFactor=0.2, tolerance=0.3)

    assert RUNNER.last_query() == "CALL gds.pageRank.stats($graph_name, $config)"
    assert RUNNER.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_pageRank_stream():
    graph = gds.graph.create(GRAPH_NAME, "Node", "REL")

    gds.pageRank.stream(graph, dampingFactor=0.2, tolerance=0.3)

    assert RUNNER.last_query() == "CALL gds.pageRank.stream($graph_name, $config)"
    assert RUNNER.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_pageRank_write():
    graph = gds.graph.create(GRAPH_NAME, "Node", "REL")

    gds.pageRank.write(graph, writeProperty="rank", dampingFactor=0.2, tolerance=0.3)

    assert RUNNER.last_query() == "CALL gds.pageRank.write($graph_name, $config)"
    assert RUNNER.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"writeProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3},
    }
