import pandas as pd
import pytest

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.k1coloring_endpoints import (
    K1ColoringMutateResult,
    K1ColoringStatsResult,
    K1ColoringWriteResult,
)
from graphdatascience.procedure_surface.cypher.k1coloring_cypher_endpoints import K1ColoringCypherEndpoints
from graphdatascience.tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner
from graphdatascience.tests.unit.procedure_surface.cypher.conftest import estimate_mock_result


@pytest.fixture
def k1coloring_endpoints(query_runner: CollectingQueryRunner) -> K1ColoringCypherEndpoints:
    return K1ColoringCypherEndpoints(query_runner)


def test_mutate_basic(graph: GraphV2) -> None:
    result = {
        "nodeCount": 5,
        "colorCount": 3,
        "ranIterations": 2,
        "didConverge": True,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "mutateMillis": 42,
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"k1coloring.mutate": pd.DataFrame([result])})

    result_obj = K1ColoringCypherEndpoints(query_runner).mutate(graph, "color")

    assert len(query_runner.queries) == 1
    assert "gds.k1coloring.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["mutateProperty"] == "color"
    assert "jobId" in config

    assert isinstance(result_obj, K1ColoringMutateResult)
    assert result_obj.node_count == 5
    assert result_obj.color_count == 3
    assert result_obj.ran_iterations == 2
    assert result_obj.did_converge is True
    assert result_obj.pre_processing_millis == 10
    assert result_obj.compute_millis == 20
    assert result_obj.mutate_millis == 42
    assert result_obj.configuration == {"bar": 1337}


def test_mutate_with_optional_params(graph: GraphV2) -> None:
    result = {
        "nodeCount": 5,
        "colorCount": 3,
        "ranIterations": 2,
        "didConverge": True,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "mutateMillis": 42,
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"k1coloring.mutate": pd.DataFrame([result])})

    K1ColoringCypherEndpoints(query_runner).mutate(
        graph,
        "color",
        batch_size=1000,
        max_iterations=10,
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
    )

    assert len(query_runner.queries) == 1
    assert "gds.k1coloring.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "mutateProperty": "color",
        "batchSize": 1000,
        "maxIterations": 10,
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
    }


def test_stats_basic(graph: GraphV2) -> None:
    result = {
        "nodeCount": 5,
        "colorCount": 3,
        "ranIterations": 2,
        "didConverge": True,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"k1coloring.stats": pd.DataFrame([result])})

    result_obj = K1ColoringCypherEndpoints(query_runner).stats(graph)

    assert len(query_runner.queries) == 1
    assert "gds.k1coloring.stats" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert "jobId" in config

    assert isinstance(result_obj, K1ColoringStatsResult)
    assert result_obj.node_count == 5
    assert result_obj.color_count == 3
    assert result_obj.ran_iterations == 2
    assert result_obj.did_converge is True
    assert result_obj.pre_processing_millis == 10
    assert result_obj.compute_millis == 20
    assert result_obj.configuration == {"bar": 1337}


def test_stats_with_optional_params(graph: GraphV2) -> None:
    result = {
        "nodeCount": 5,
        "colorCount": 3,
        "ranIterations": 2,
        "didConverge": True,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"k1coloring.stats": pd.DataFrame([result])})

    K1ColoringCypherEndpoints(query_runner).stats(
        graph,
        batch_size=1000,
        max_iterations=10,
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
    )

    assert len(query_runner.queries) == 1
    assert "gds.k1coloring.stats" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "batchSize": 1000,
        "maxIterations": 10,
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
    }


def test_stream_basic(
    k1coloring_endpoints: K1ColoringCypherEndpoints, graph: GraphV2, query_runner: CollectingQueryRunner
) -> None:
    k1coloring_endpoints.stream(graph)

    assert len(query_runner.queries) == 1
    assert "gds.k1coloring.stream" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert "jobId" in config


def test_stream_with_optional_params(
    k1coloring_endpoints: K1ColoringCypherEndpoints, graph: GraphV2, query_runner: CollectingQueryRunner
) -> None:
    k1coloring_endpoints.stream(
        graph,
        batch_size=1000,
        max_iterations=10,
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
        min_community_size=2,
    )

    assert len(query_runner.queries) == 1
    assert "gds.k1coloring.stream" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "batchSize": 1000,
        "maxIterations": 10,
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
        "minCommunitySize": 2,
    }


def test_write_basic(graph: GraphV2) -> None:
    result = {
        "nodeCount": 5,
        "colorCount": 3,
        "ranIterations": 2,
        "didConverge": True,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "writeMillis": 42,
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"k1coloring.write": pd.DataFrame([result])})

    result_obj = K1ColoringCypherEndpoints(query_runner).write(graph, "color")

    assert len(query_runner.queries) == 1
    assert "gds.k1coloring.write" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["writeProperty"] == "color"
    assert "jobId" in config

    assert isinstance(result_obj, K1ColoringWriteResult)
    assert result_obj.node_count == 5
    assert result_obj.color_count == 3
    assert result_obj.ran_iterations == 2
    assert result_obj.did_converge is True
    assert result_obj.pre_processing_millis == 10
    assert result_obj.compute_millis == 20
    assert result_obj.write_millis == 42
    assert result_obj.configuration == {"bar": 1337}


def test_write_with_optional_params(graph: GraphV2) -> None:
    result = {
        "nodeCount": 5,
        "colorCount": 3,
        "ranIterations": 2,
        "didConverge": True,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "writeMillis": 42,
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"k1coloring.write": pd.DataFrame([result])})

    K1ColoringCypherEndpoints(query_runner).write(
        graph,
        "color",
        batch_size=1000,
        max_iterations=10,
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
        write_concurrency=2,
        min_community_size=2,
    )

    assert len(query_runner.queries) == 1
    assert "gds.k1coloring.write" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "writeProperty": "color",
        "batchSize": 1000,
        "maxIterations": 10,
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
        "writeConcurrency": 2,
        "minCommunitySize": 2,
    }


def test_estimate_with_graph_name(graph: GraphV2) -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"k1coloring.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    K1ColoringCypherEndpoints(query_runner).estimate(G=graph)

    assert len(query_runner.queries) == 1
    assert "gds.k1coloring.stats.estimate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graphNameOrConfiguration"] == "test_graph"
    assert params["algoConfig"] == {}


def test_estimate_with_projection_config() -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"k1coloring.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    K1ColoringCypherEndpoints(query_runner).estimate(G={"foo": "bar"})

    assert len(query_runner.queries) == 1
    assert "gds.k1coloring.stats.estimate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graphNameOrConfiguration"] == {"foo": "bar"}
    assert params["algoConfig"] == {}
