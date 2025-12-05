import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.tests.unit.conftest import CollectingQueryRunner

GRAPH_NAME = "g"


@pytest.fixture
def G(gds: GraphDataScience) -> Graph:
    G_, _ = gds.graph.project(GRAPH_NAME, "Node", "REL")
    return G_


def test_simple_mutate(runner: CollectingQueryRunner, gds: GraphDataScience, G: Graph) -> None:
    gds.algoName.mutate(G, mutateProperty="rank", dampingFactor=0.2, tolerance=0.3)

    assert runner.last_query() == "CALL gds.algoName.mutate($graph_name, $config)"
    jobId = runner.last_params().get("config", {}).get("jobId", "")
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"mutateProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3, "jobId": jobId},
    }


def test_simple_stats(runner: CollectingQueryRunner, gds: GraphDataScience, G: Graph) -> None:
    gds.algoName.stats(G, dampingFactor=0.2, tolerance=0.3)

    assert runner.last_query() == "CALL gds.algoName.stats($graph_name, $config)"
    jobId = runner.last_params().get("config", {}).get("jobId", "")
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"dampingFactor": 0.2, "tolerance": 0.3, "jobId": jobId},
    }


def test_simple_stream(runner: CollectingQueryRunner, gds: GraphDataScience, G: Graph) -> None:
    gds.algoName.stream(G, dampingFactor=0.2, tolerance=0.3)

    assert runner.last_query() == "CALL gds.algoName.stream($graph_name, $config)"
    jobId = runner.last_params().get("config", {}).get("jobId", "")
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"dampingFactor": 0.2, "tolerance": 0.3, "jobId": jobId},
    }


def test_simple_write(runner: CollectingQueryRunner, gds: GraphDataScience, G: Graph) -> None:
    gds.algoName.write(G, writeProperty="rank", dampingFactor=0.2, tolerance=0.3)

    assert runner.last_query() == "CALL gds.algoName.write($graph_name, $config)"
    jobId = runner.last_params().get("config", {}).get("jobId", "")
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"writeProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3, "jobId": jobId},
    }


def test_simple_mutate_estimate(runner: CollectingQueryRunner, gds: GraphDataScience, G: Graph) -> None:
    gds.algoName.mutate.estimate(G, mutateProperty="rank", dampingFactor=0.2, tolerance=0.3)

    assert runner.last_query() == "CALL gds.algoName.mutate.estimate($graph_name, $config)"
    jobId = runner.last_params().get("config", {}).get("jobId", "")
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"mutateProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3, "jobId": jobId},
    }


def test_simple_stats_estimate(runner: CollectingQueryRunner, gds: GraphDataScience, G: Graph) -> None:
    gds.algoName.stats.estimate(G, dampingFactor=0.2, tolerance=0.3)

    assert runner.last_query() == "CALL gds.algoName.stats.estimate($graph_name, $config)"
    jobId = runner.last_params().get("config", {}).get("jobId", "")
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"dampingFactor": 0.2, "tolerance": 0.3, "jobId": jobId},
    }


def test_simple_stream_estimate(runner: CollectingQueryRunner, gds: GraphDataScience, G: Graph) -> None:
    gds.algoName.stream.estimate(G, dampingFactor=0.2, tolerance=0.3)

    assert runner.last_query() == "CALL gds.algoName.stream.estimate($graph_name, $config)"
    jobId = runner.last_params().get("config", {}).get("jobId", "")
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"dampingFactor": 0.2, "tolerance": 0.3, "jobId": jobId},
    }


def test_simple_write_estimate(runner: CollectingQueryRunner, gds: GraphDataScience, G: Graph) -> None:
    gds.algoName.write.estimate(G, writeProperty="rank", dampingFactor=0.2, tolerance=0.3)

    assert runner.last_query() == "CALL gds.algoName.write.estimate($graph_name, $config)"
    jobId = runner.last_params().get("config", {}).get("jobId", "")
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"writeProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3, "jobId": jobId},
    }
