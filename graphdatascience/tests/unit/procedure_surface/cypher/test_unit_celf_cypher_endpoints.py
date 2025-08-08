import pandas as pd
import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.procedure_surface.api.celf_endpoints import (
    CelfMutateResult,
    CelfStatsResult,
    CelfWriteResult,
)
from graphdatascience.procedure_surface.cypher.celf_cypher_endpoints import CelfCypherEndpoints
from graphdatascience.tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner
from graphdatascience.tests.unit.procedure_surface.cypher.conftests import estimate_mock_result


@pytest.fixture
def query_runner() -> CollectingQueryRunner:
    return CollectingQueryRunner(DEFAULT_SERVER_VERSION, {})


@pytest.fixture
def celf_endpoints(query_runner: CollectingQueryRunner) -> CelfCypherEndpoints:
    return CelfCypherEndpoints(query_runner)


@pytest.fixture
def graph(query_runner: CollectingQueryRunner) -> Graph:
    return Graph("test_graph", query_runner)


def test_mutate_basic(graph: Graph) -> None:
    result = {
        "nodePropertiesWritten": 5,
        "mutateMillis": 42,
        "computeMillis": 20,
        "totalSpread": 15.5,
        "nodeCount": 100,
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"influenceMaximization.celf.mutate": pd.DataFrame([result])}
    )

    result_obj = CelfCypherEndpoints(query_runner).mutate(graph, 5, "celf_influence")

    assert len(query_runner.queries) == 1
    assert "gds.influenceMaximization.celf.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["seedSetSize"] == 5
    assert config["mutateProperty"] == "celf_influence"
    assert "jobId" in config

    assert isinstance(result_obj, CelfMutateResult)
    assert result_obj.node_properties_written == 5
    assert result_obj.mutate_millis == 42
    assert result_obj.compute_millis == 20
    assert result_obj.total_spread == 15.5
    assert result_obj.node_count == 100
    assert result_obj.configuration == {"bar": 1337}


def test_mutate_with_optional_params(graph: Graph) -> None:
    result = {
        "nodePropertiesWritten": 3,
        "mutateMillis": 35,
        "computeMillis": 18,
        "totalSpread": 12.3,
        "nodeCount": 50,
        "configuration": {"foo": 42},
    }

    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"influenceMaximization.celf.mutate": pd.DataFrame([result])}
    )

    CelfCypherEndpoints(query_runner).mutate(
        graph,
        3,
        "celf_influence",
        propagation_probability=0.1,
        monte_carlo_simulations=100,
        random_seed=42,
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
    )

    assert len(query_runner.queries) == 1
    assert "gds.influenceMaximization.celf.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "seedSetSize": 3,
        "mutateProperty": "celf_influence",
        "propagationProbability": 0.1,
        "monteCarloSimulations": 100,
        "randomSeed": 42,
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
    }


def test_stats_basic(graph: Graph) -> None:
    result = {
        "computeMillis": 20,
        "totalSpread": 15.5,
        "nodeCount": 100,
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"influenceMaximization.celf.stats": pd.DataFrame([result])}
    )

    result_obj = CelfCypherEndpoints(query_runner).stats(graph, 5)

    assert len(query_runner.queries) == 1
    assert "gds.influenceMaximization.celf.stats" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["seedSetSize"] == 5
    assert "jobId" in config

    assert isinstance(result_obj, CelfStatsResult)
    assert result_obj.compute_millis == 20
    assert result_obj.total_spread == 15.5
    assert result_obj.node_count == 100
    assert result_obj.configuration == {"bar": 1337}


def test_stream_basic(celf_endpoints: CelfCypherEndpoints, graph: Graph, query_runner: CollectingQueryRunner) -> None:
    celf_endpoints.stream(graph, 3)

    assert len(query_runner.queries) == 1
    assert "gds.influenceMaximization.celf.stream" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["seedSetSize"] == 3
    assert "jobId" in config


def test_write_basic(graph: Graph) -> None:
    result = {
        "nodePropertiesWritten": 5,
        "writeMillis": 42,
        "computeMillis": 20,
        "totalSpread": 15.5,
        "nodeCount": 100,
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"influenceMaximization.celf.write": pd.DataFrame([result])}
    )

    result_obj = CelfCypherEndpoints(query_runner).write(graph, 5, "celf_influence")

    assert len(query_runner.queries) == 1
    assert "gds.influenceMaximization.celf.write" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["seedSetSize"] == 5
    assert config["writeProperty"] == "celf_influence"
    assert "jobId" in config

    assert isinstance(result_obj, CelfWriteResult)
    assert result_obj.node_properties_written == 5
    assert result_obj.write_millis == 42
    assert result_obj.compute_millis == 20
    assert result_obj.total_spread == 15.5
    assert result_obj.node_count == 100
    assert result_obj.configuration == {"bar": 1337}


def test_estimate_with_graph_name(graph: Graph) -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"influenceMaximization.celf.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    CelfCypherEndpoints(query_runner).estimate(graph, 5)

    assert len(query_runner.queries) == 1
    assert "gds.influenceMaximization.celf.stats.estimate" in query_runner.queries[0]


def test_estimate_with_projection_config(query_runner: CollectingQueryRunner) -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"influenceMaximization.celf.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    projection_config = {
        "nodeProjection": "*",
        "relationshipProjection": "*",
    }

    CelfCypherEndpoints(query_runner).estimate(projection_config, 3, propagation_probability=0.1)

    assert len(query_runner.queries) == 1
    assert "gds.influenceMaximization.celf.stats.estimate" in query_runner.queries[0]
