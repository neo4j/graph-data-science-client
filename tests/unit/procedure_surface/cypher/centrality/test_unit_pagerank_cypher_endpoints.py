import pandas as pd
import pytest

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.centrality.pagerank_endpoints import (
    PageRankMutateResult,
    PageRankStatsResult,
    PageRankWriteResult,
)
from graphdatascience.procedure_surface.cypher.centrality.pagerank_cypher_endpoints import PageRankCypherEndpoints
from tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner
from tests.unit.procedure_surface.cypher.conftest import estimate_mock_result


@pytest.fixture
def pagerank_endpoints(query_runner: CollectingQueryRunner) -> PageRankCypherEndpoints:
    return PageRankCypherEndpoints(query_runner)


def test_mutate_basic(graph: GraphV2) -> None:
    result = {
        "nodePropertiesWritten": 5,
        "mutateMillis": 42,
        "ranIterations": 20,
        "didConverge": True,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "centralityDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"pageRank.mutate": pd.DataFrame([result])})

    result_obj = PageRankCypherEndpoints(query_runner).mutate(graph, "pagerank")

    assert len(query_runner.queries) == 1
    assert "gds.pageRank.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["mutateProperty"] == "pagerank"
    assert "jobId" in config

    assert isinstance(result_obj, PageRankMutateResult)
    assert result_obj.node_properties_written == 5
    assert result_obj.mutate_millis == 42
    assert result_obj.ran_iterations == 20
    assert result_obj.did_converge is True
    assert result_obj.pre_processing_millis == 10
    assert result_obj.compute_millis == 20
    assert result_obj.post_processing_millis == 12
    assert result_obj.centrality_distribution == {"foo": 42}
    assert result_obj.configuration == {"bar": 1337}


def test_mutate_with_optional_params(graph: GraphV2) -> None:
    result = {
        "nodePropertiesWritten": 5,
        "mutateMillis": 42,
        "ranIterations": 20,
        "didConverge": True,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "centralityDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"pageRank.mutate": pd.DataFrame([result])})

    PageRankCypherEndpoints(query_runner).mutate(
        graph,
        "pagerank",
        damping_factor=0.85,
        tolerance=0.0001,
        max_iterations=20,
        scaler={"type": "L2Norm"},
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
        relationship_weight_property="weight",
        source_nodes=[1, 2, 3],
    )

    assert len(query_runner.queries) == 1
    assert "gds.pageRank.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "mutateProperty": "pagerank",
        "dampingFactor": 0.85,
        "tolerance": 0.0001,
        "maxIterations": 20,
        "scaler": {"type": "L2Norm"},
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
        "relationshipWeightProperty": "weight",
        "sourceNodes": [1, 2, 3],
    }


def test_stats_basic(graph: GraphV2) -> None:
    result = {
        "ranIterations": 20,
        "didConverge": True,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "centralityDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"pageRank.stats": pd.DataFrame([result])})

    result_obj = PageRankCypherEndpoints(query_runner).stats(graph)

    assert len(query_runner.queries) == 1
    assert "gds.pageRank.stats" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert "jobId" in config

    assert isinstance(result_obj, PageRankStatsResult)
    assert result_obj.ran_iterations == 20
    assert result_obj.did_converge is True
    assert result_obj.pre_processing_millis == 10
    assert result_obj.compute_millis == 20
    assert result_obj.post_processing_millis == 12
    assert result_obj.centrality_distribution == {"foo": 42}
    assert result_obj.configuration == {"bar": 1337}


def test_stats_with_optional_params(graph: GraphV2) -> None:
    result = {
        "ranIterations": 20,
        "didConverge": True,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "centralityDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"pageRank.stats": pd.DataFrame([result])})

    PageRankCypherEndpoints(query_runner).stats(
        graph,
        damping_factor=0.85,
        tolerance=0.0001,
        max_iterations=20,
        scaler={"type": "L2Norm"},
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
        relationship_weight_property="weight",
        source_nodes=[1, 2, 3],
    )

    assert len(query_runner.queries) == 1
    assert "gds.pageRank.stats" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "dampingFactor": 0.85,
        "tolerance": 0.0001,
        "maxIterations": 20,
        "scaler": {"type": "L2Norm"},
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
        "relationshipWeightProperty": "weight",
        "sourceNodes": [1, 2, 3],
    }


def test_stream_basic(
    pagerank_endpoints: PageRankCypherEndpoints, graph: GraphV2, query_runner: CollectingQueryRunner
) -> None:
    pagerank_endpoints.stream(graph)

    assert len(query_runner.queries) == 1
    assert "gds.pageRank.stream" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert "jobId" in config


def test_stream_with_optional_params(
    pagerank_endpoints: PageRankCypherEndpoints, graph: GraphV2, query_runner: CollectingQueryRunner
) -> None:
    pagerank_endpoints.stream(
        graph,
        damping_factor=0.85,
        tolerance=0.0001,
        max_iterations=20,
        scaler={"type": "L2Norm"},
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
        relationship_weight_property="weight",
        source_nodes=[1, 2, 3],
    )

    assert len(query_runner.queries) == 1
    assert "gds.pageRank.stream" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "dampingFactor": 0.85,
        "tolerance": 0.0001,
        "maxIterations": 20,
        "scaler": {"type": "L2Norm"},
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
        "relationshipWeightProperty": "weight",
        "sourceNodes": [1, 2, 3],
    }


def test_write_basic(graph: GraphV2) -> None:
    result = {
        "ranIterations": 20,
        "didConverge": True,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "writeMillis": 15,
        "postProcessingMillis": 12,
        "nodePropertiesWritten": 5,
        "centralityDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"pageRank.write": pd.DataFrame([result])})

    result_obj = PageRankCypherEndpoints(query_runner).write(graph, "pagerank")

    assert len(query_runner.queries) == 1
    assert "gds.pageRank.write" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["writeProperty"] == "pagerank"
    assert "jobId" in config

    assert isinstance(result_obj, PageRankWriteResult)
    assert result_obj.ran_iterations == 20
    assert result_obj.did_converge is True
    assert result_obj.pre_processing_millis == 10
    assert result_obj.compute_millis == 20
    assert result_obj.write_millis == 15
    assert result_obj.post_processing_millis == 12
    assert result_obj.node_properties_written == 5
    assert result_obj.centrality_distribution == {"foo": 42}
    assert result_obj.configuration == {"bar": 1337}


def test_write_with_optional_params(graph: GraphV2) -> None:
    result = {
        "ranIterations": 20,
        "didConverge": True,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "writeMillis": 15,
        "postProcessingMillis": 12,
        "nodePropertiesWritten": 5,
        "centralityDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"pageRank.write": pd.DataFrame([result])})

    PageRankCypherEndpoints(query_runner).write(
        graph,
        "pagerank",
        damping_factor=0.85,
        tolerance=0.0001,
        max_iterations=20,
        scaler={"type": "L2Norm"},
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
        relationship_weight_property="weight",
        source_nodes=[1, 2, 3],
        write_concurrency=4,
    )

    assert len(query_runner.queries) == 1
    assert "gds.pageRank.write" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "writeProperty": "pagerank",
        "dampingFactor": 0.85,
        "tolerance": 0.0001,
        "maxIterations": 20,
        "scaler": {"type": "L2Norm"},
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
        "relationshipWeightProperty": "weight",
        "sourceNodes": [1, 2, 3],
        "writeConcurrency": 4,
    }


def test_estimate_with_graph_name(graph: GraphV2) -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"pageRank.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    estimate = PageRankCypherEndpoints(query_runner).estimate(G=graph)

    assert estimate.node_count == 100

    assert len(query_runner.queries) == 1
    assert "gds.pageRank.stats.estimate" in query_runner.queries[0]


def test_estimate_with_projection_config() -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"pageRank.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    estimate = PageRankCypherEndpoints(query_runner).estimate(G={"foo": "bar"})

    assert estimate.node_count == 100

    assert len(query_runner.queries) == 1
    assert "gds.pageRank.stats.estimate" in query_runner.queries[0]
