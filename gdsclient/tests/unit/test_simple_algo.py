from gdsclient import GraphDataScience

from . import CollectingQueryRunner

GRAPH_NAME = "g"


def setup_module():
    global runner
    global gds
    global G

    runner = CollectingQueryRunner()
    gds = GraphDataScience(runner)
    G = gds.graph.project(GRAPH_NAME, "Node", "REL")


def test_algoName_mutate():
    gds.algoName.mutate(G, mutateProperty="rank", dampingFactor=0.2, tolerance=0.3)

    assert runner.last_query() == "CALL gds.algoName.mutate($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"mutateProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_stats():
    gds.algoName.stats(G, dampingFactor=0.2, tolerance=0.3)

    assert runner.last_query() == "CALL gds.algoName.stats($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_stream():
    gds.algoName.stream(G, dampingFactor=0.2, tolerance=0.3)

    assert runner.last_query() == "CALL gds.algoName.stream($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_write():
    gds.algoName.write(G, writeProperty="rank", dampingFactor=0.2, tolerance=0.3)

    assert runner.last_query() == "CALL gds.algoName.write($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"writeProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_mutate_estimate():
    gds.algoName.mutate.estimate(
        G, mutateProperty="rank", dampingFactor=0.2, tolerance=0.3
    )

    assert (
        runner.last_query() == "CALL gds.algoName.mutate.estimate($graph_name, $config)"
    )
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"mutateProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_stats_estimate():
    gds.algoName.stats.estimate(G, dampingFactor=0.2, tolerance=0.3)

    assert (
        runner.last_query() == "CALL gds.algoName.stats.estimate($graph_name, $config)"
    )
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_stream_estimate():
    gds.algoName.stream.estimate(G, dampingFactor=0.2, tolerance=0.3)

    assert (
        runner.last_query() == "CALL gds.algoName.stream.estimate($graph_name, $config)"
    )
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_algoName_write_estimate():
    gds.algoName.write.estimate(
        G, writeProperty="rank", dampingFactor=0.2, tolerance=0.3
    )

    assert (
        runner.last_query() == "CALL gds.algoName.write.estimate($graph_name, $config)"
    )
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"writeProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3},
    }
