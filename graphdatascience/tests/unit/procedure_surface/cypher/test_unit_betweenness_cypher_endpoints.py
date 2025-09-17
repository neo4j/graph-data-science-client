import pandas as pd
import pytest

from graphdatascience.procedure_surface.api.betweenness_endpoints import (
    BetweennessMutateResult,
    BetweennessStatsResult,
    BetweennessWriteResult,
)
from graphdatascience.procedure_surface.api.catalog.graph_api import Graph
from graphdatascience.procedure_surface.cypher.betweenness_cypher_endpoints import BetweennessCypherEndpoints
from graphdatascience.tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner
from graphdatascience.tests.unit.procedure_surface.cypher.conftests import estimate_mock_result


@pytest.fixture
def betweenness_endpoints(query_runner: CollectingQueryRunner) -> BetweennessCypherEndpoints:
    return BetweennessCypherEndpoints(query_runner)


def test_mutate_basic(graph: Graph) -> None:
    result = {
        "nodePropertiesWritten": 5,
        "mutateMillis": 42,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "centralityDistribution": {"p50": 0.5, "p90": 0.9, "p99": 0.99},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"betweenness.mutate": pd.DataFrame([result])})

    result_obj = BetweennessCypherEndpoints(query_runner).mutate(graph, "betweenness")

    assert len(query_runner.queries) == 1
    assert "gds.betweenness.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["mutateProperty"] == "betweenness"
    assert "jobId" in config

    assert isinstance(result_obj, BetweennessMutateResult)
    assert result_obj.node_properties_written == 5
    assert result_obj.mutate_millis == 42
    assert result_obj.pre_processing_millis == 10
    assert result_obj.compute_millis == 20
    assert result_obj.post_processing_millis == 12
    assert result_obj.centrality_distribution == {"p50": 0.5, "p90": 0.9, "p99": 0.99}
    assert result_obj.configuration == {"bar": 1337}


def test_mutate_with_optional_params(graph: Graph) -> None:
    result = {
        "nodePropertiesWritten": 5,
        "mutateMillis": 42,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "centralityDistribution": {"p50": 0.5, "p90": 0.9, "p99": 0.99},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"betweenness.mutate": pd.DataFrame([result])})

    BetweennessCypherEndpoints(query_runner).mutate(
        graph,
        "betweenness",
        sampling_size=1000,
        sampling_seed=42,
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
        relationship_weight_property="weight",
    )

    assert len(query_runner.queries) == 1
    assert "gds.betweenness.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "mutateProperty": "betweenness",
        "samplingSize": 1000,
        "samplingSeed": 42,
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
        "relationshipWeightProperty": "weight",
    }


def test_stats_basic(graph: Graph) -> None:
    result = {
        "centralityDistribution": {"p50": 0.5, "p90": 0.9, "p99": 0.99},
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"betweenness.stats": pd.DataFrame([result])})

    result_obj = BetweennessCypherEndpoints(query_runner).stats(graph)

    assert len(query_runner.queries) == 1
    assert "gds.betweenness.stats" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert "jobId" in config

    assert isinstance(result_obj, BetweennessStatsResult)
    assert result_obj.centrality_distribution == {"p50": 0.5, "p90": 0.9, "p99": 0.99}
    assert result_obj.pre_processing_millis == 10
    assert result_obj.compute_millis == 20
    assert result_obj.post_processing_millis == 12
    assert result_obj.configuration == {"bar": 1337}


def test_stats_with_optional_params(graph: Graph) -> None:
    result = {
        "centralityDistribution": {"p50": 0.5, "p90": 0.9, "p99": 0.99},
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"betweenness.stats": pd.DataFrame([result])})

    BetweennessCypherEndpoints(query_runner).stats(
        graph,
        sampling_size=1000,
        sampling_seed=42,
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
        relationship_weight_property="weight",
    )

    assert len(query_runner.queries) == 1
    assert "gds.betweenness.stats" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "samplingSize": 1000,
        "samplingSeed": 42,
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
        "relationshipWeightProperty": "weight",
    }


def test_stream_basic(
    betweenness_endpoints: BetweennessCypherEndpoints, graph: Graph, query_runner: CollectingQueryRunner
) -> None:
    result_data = pd.DataFrame({"nodeId": [1, 2, 3], "score": [0.1, 0.8, 0.3]})
    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"betweenness.stream": result_data})

    result = BetweennessCypherEndpoints(query_runner).stream(graph)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 3
    assert "nodeId" in result.columns
    assert "score" in result.columns

    assert len(query_runner.queries) == 1
    assert "gds.betweenness.stream" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert "jobId" in config


def test_stream_with_optional_params(
    betweenness_endpoints: BetweennessCypherEndpoints, graph: Graph, query_runner: CollectingQueryRunner
) -> None:
    BetweennessCypherEndpoints(query_runner).stream(
        graph,
        sampling_size=1000,
        sampling_seed=42,
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
        relationship_weight_property="weight",
    )

    assert len(query_runner.queries) == 1
    assert "gds.betweenness.stream" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "samplingSize": 1000,
        "samplingSeed": 42,
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
        "relationshipWeightProperty": "weight",
    }


def test_write_basic(graph: Graph) -> None:
    result = {
        "nodePropertiesWritten": 5,
        "writeMillis": 15,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "centralityDistribution": {"p50": 0.5, "p90": 0.9, "p99": 0.99},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"betweenness.write": pd.DataFrame([result])})

    result_obj = BetweennessCypherEndpoints(query_runner).write(graph, "betweenness")

    assert len(query_runner.queries) == 1
    assert "gds.betweenness.write" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["writeProperty"] == "betweenness"
    assert "jobId" in config

    assert isinstance(result_obj, BetweennessWriteResult)
    assert result_obj.node_properties_written == 5
    assert result_obj.write_millis == 15
    assert result_obj.pre_processing_millis == 10
    assert result_obj.compute_millis == 20
    assert result_obj.post_processing_millis == 12
    assert result_obj.centrality_distribution == {"p50": 0.5, "p90": 0.9, "p99": 0.99}
    assert result_obj.configuration == {"bar": 1337}


def test_write_with_optional_params(graph: Graph) -> None:
    result = {
        "nodePropertiesWritten": 5,
        "writeMillis": 15,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "centralityDistribution": {"p50": 0.5, "p90": 0.9, "p99": 0.99},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"betweenness.write": pd.DataFrame([result])})

    BetweennessCypherEndpoints(query_runner).write(
        graph,
        "betweenness",
        sampling_size=1000,
        sampling_seed=42,
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
        relationship_weight_property="weight",
        write_concurrency=4,
    )

    assert len(query_runner.queries) == 1
    assert "gds.betweenness.write" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "writeProperty": "betweenness",
        "samplingSize": 1000,
        "samplingSeed": 42,
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
        "relationshipWeightProperty": "weight",
        "writeConcurrency": 4,
    }


def test_estimate_with_graph_name(graph: Graph) -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"betweenness.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    BetweennessCypherEndpoints(query_runner).estimate(graph)

    assert len(query_runner.queries) == 1
    assert "gds.betweenness.stats.estimate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graphNameOrConfiguration"] == "test_graph"


def test_estimate_with_projection_config() -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"betweenness.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    projection_config = {"nodeProjection": "Node", "relationshipProjection": "REL"}
    BetweennessCypherEndpoints(query_runner).estimate(G=projection_config)

    assert len(query_runner.queries) == 1
    assert "gds.betweenness.stats.estimate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graphNameOrConfiguration"] == projection_config
