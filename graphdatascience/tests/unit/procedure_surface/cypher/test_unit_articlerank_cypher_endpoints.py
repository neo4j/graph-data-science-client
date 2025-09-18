import pandas as pd
import pytest

from graphdatascience.procedure_surface.api.centrality.articlerank_endpoints import (
    ArticleRankMutateResult,
    ArticleRankStatsResult,
    ArticleRankWriteResult,
)
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.articlerank_cypher_endpoints import ArticleRankCypherEndpoints
from graphdatascience.tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner
from graphdatascience.tests.unit.procedure_surface.cypher.conftest import estimate_mock_result


@pytest.fixture
def articlerank_endpoints(query_runner: CollectingQueryRunner) -> ArticleRankCypherEndpoints:
    return ArticleRankCypherEndpoints(query_runner)


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

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"articleRank.mutate": pd.DataFrame([result])})

    result_obj = ArticleRankCypherEndpoints(query_runner).mutate(graph, "articlerank")

    assert len(query_runner.queries) == 1
    assert "gds.articleRank.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["mutateProperty"] == "articlerank"
    assert "jobId" in config

    assert isinstance(result_obj, ArticleRankMutateResult)
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

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"articleRank.mutate": pd.DataFrame([result])})

    ArticleRankCypherEndpoints(query_runner).mutate(
        graph,
        "articlerank",
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
    assert "gds.articleRank.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "mutateProperty": "articlerank",
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

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"articleRank.stats": pd.DataFrame([result])})

    result_obj = ArticleRankCypherEndpoints(query_runner).stats(graph)

    assert len(query_runner.queries) == 1
    assert "gds.articleRank.stats" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert "jobId" in params["config"]

    assert isinstance(result_obj, ArticleRankStatsResult)
    assert result_obj.ran_iterations == 20
    assert result_obj.did_converge is True
    assert result_obj.pre_processing_millis == 10
    assert result_obj.compute_millis == 20
    assert result_obj.post_processing_millis == 12
    assert result_obj.centrality_distribution == {"foo": 42}
    assert result_obj.configuration == {"bar": 1337}


def test_stream_basic(
    articlerank_endpoints: ArticleRankCypherEndpoints, graph: GraphV2, query_runner: CollectingQueryRunner
) -> None:
    articlerank_endpoints.stream(graph)

    assert len(query_runner.queries) == 1
    assert "gds.articleRank.stream" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert "jobId" in config


def test_write_basic(graph: GraphV2) -> None:
    result = {
        "nodePropertiesWritten": 5,
        "writeMillis": 42,
        "ranIterations": 20,
        "didConverge": True,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "centralityDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"articleRank.write": pd.DataFrame([result])})

    result_obj = ArticleRankCypherEndpoints(query_runner).write(graph, "articlerank")

    assert len(query_runner.queries) == 1
    assert "gds.articleRank.write" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["writeProperty"] == "articlerank"
    assert "jobId" in config

    assert isinstance(result_obj, ArticleRankWriteResult)
    assert result_obj.node_properties_written == 5
    assert result_obj.write_millis == 42


def test_estimate_with_graph_name(graph: GraphV2) -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"articleRank.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    result = ArticleRankCypherEndpoints(query_runner).estimate(graph)

    assert len(query_runner.queries) == 1
    assert "gds.articleRank.stats.estimate" in query_runner.queries[0]
    assert result.node_count == 100
