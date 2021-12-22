from gdsclient import GraphDataScience

from . import CollectingQueryRunner

GRAPH_NAME = "g"


def setup_module():
    global runner
    global gds
    global graph

    runner = CollectingQueryRunner()
    gds = GraphDataScience(runner)
    graph = gds.graph.project(GRAPH_NAME, "Node", "REL")


def test_algoName_mutate():
    gds.algoName.mutate(graph, mutateProperty="rank", dampingFactor=0.2, tolerance=0.3)

    assert runner.last_query() == "CALL gds.algoName.mutate($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"mutateProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_stats():
    gds.algoName.stats(graph, dampingFactor=0.2, tolerance=0.3)

    assert runner.last_query() == "CALL gds.algoName.stats($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_stream():
    gds.algoName.stream(graph, dampingFactor=0.2, tolerance=0.3)

    assert runner.last_query() == "CALL gds.algoName.stream($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_write():
    gds.algoName.write(graph, writeProperty="rank", dampingFactor=0.2, tolerance=0.3)

    assert runner.last_query() == "CALL gds.algoName.write($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"writeProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_mutate_estimate():
    gds.algoName.mutate.estimate(
        graph, mutateProperty="rank", dampingFactor=0.2, tolerance=0.3
    )

    assert (
        runner.last_query() == "CALL gds.algoName.mutate.estimate($graph_name, $config)"
    )
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"mutateProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_stats_estimate():
    gds.algoName.stats.estimate(graph, dampingFactor=0.2, tolerance=0.3)

    assert (
        runner.last_query() == "CALL gds.algoName.stats.estimate($graph_name, $config)"
    )
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_stream_estimate():
    gds.algoName.stream.estimate(graph, dampingFactor=0.2, tolerance=0.3)

    assert (
        runner.last_query() == "CALL gds.algoName.stream.estimate($graph_name, $config)"
    )
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_write_estimate():
    gds.algoName.write.estimate(
        graph, writeProperty="rank", dampingFactor=0.2, tolerance=0.3
    )

    assert (
        runner.last_query() == "CALL gds.algoName.write.estimate($graph_name, $config)"
    )
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"writeProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3},
    }
