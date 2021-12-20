from gdsclient import GraphDataScience
from . import CollectingQueryRunner


RUNNER = CollectingQueryRunner()
gds = GraphDataScience(RUNNER)
GRAPH_NAME = "g"
GRAPH = gds.graph.create(GRAPH_NAME, "Node", "REL")


def test_algoName_mutate():
    gds.algoName.mutate(GRAPH, mutateProperty="rank", dampingFactor=0.2, tolerance=0.3)

    assert RUNNER.last_query() == "CALL gds.algoName.mutate($graph_name, $config)"
    assert RUNNER.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"mutateProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_stats():
    gds.algoName.stats(GRAPH, dampingFactor=0.2, tolerance=0.3)

    assert RUNNER.last_query() == "CALL gds.algoName.stats($graph_name, $config)"
    assert RUNNER.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_stream():
    gds.algoName.stream(GRAPH, dampingFactor=0.2, tolerance=0.3)

    assert RUNNER.last_query() == "CALL gds.algoName.stream($graph_name, $config)"
    assert RUNNER.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_write():
    gds.algoName.write(GRAPH, writeProperty="rank", dampingFactor=0.2, tolerance=0.3)

    assert RUNNER.last_query() == "CALL gds.algoName.write($graph_name, $config)"
    assert RUNNER.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"writeProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_mutate_estimate():
    gds.algoName.mutate.estimate(
        GRAPH, mutateProperty="rank", dampingFactor=0.2, tolerance=0.3
    )

    assert (
        RUNNER.last_query() == "CALL gds.algoName.mutate.estimate($graph_name, $config)"
    )
    assert RUNNER.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"mutateProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_stats_estimate():
    gds.algoName.stats.estimate(GRAPH, dampingFactor=0.2, tolerance=0.3)

    assert (
        RUNNER.last_query() == "CALL gds.algoName.stats.estimate($graph_name, $config)"
    )
    assert RUNNER.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_stream_estimate():
    gds.algoName.stream.estimate(GRAPH, dampingFactor=0.2, tolerance=0.3)

    assert (
        RUNNER.last_query() == "CALL gds.algoName.stream.estimate($graph_name, $config)"
    )
    assert RUNNER.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_write_estimate():
    gds.algoName.write.estimate(
        GRAPH, writeProperty="rank", dampingFactor=0.2, tolerance=0.3
    )

    assert (
        RUNNER.last_query() == "CALL gds.algoName.write.estimate($graph_name, $config)"
    )
    assert RUNNER.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"writeProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3},
    }
