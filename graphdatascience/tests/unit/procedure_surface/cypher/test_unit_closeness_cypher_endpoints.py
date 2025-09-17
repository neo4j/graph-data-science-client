import pandas as pd
import pytest

from graphdatascience.procedure_surface.api.catalog.graph_api import Graph
from graphdatascience.procedure_surface.api.closeness_endpoints import (
    ClosenessStatsResult,
    ClosenessWriteResult,
)
from graphdatascience.procedure_surface.cypher.closeness_cypher_endpoints import ClosenessCypherEndpoints
from graphdatascience.tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner
from graphdatascience.tests.unit.procedure_surface.cypher.conftests import estimate_mock_result


@pytest.fixture
def closeness_endpoints(query_runner: CollectingQueryRunner) -> ClosenessCypherEndpoints:
    return ClosenessCypherEndpoints(query_runner)


def test_mutate(graph: Graph) -> None:
    result = {
        "nodePropertiesWritten": 5,
        "mutateMillis": 42,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "centralityDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"closeness.mutate": pd.DataFrame([result])})

    ClosenessCypherEndpoints(query_runner).mutate(
        graph,
        "closeness",
        use_wasserman_faust=True,
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
    )

    assert len(query_runner.queries) == 1
    assert "gds.closeness.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "mutateProperty": "closeness",
        "useWassermanFaust": True,
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
    }


def test_stats(graph: Graph) -> None:
    result = {
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "centralityDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"closeness.stats": pd.DataFrame([result])})

    result_obj = ClosenessCypherEndpoints(query_runner).stats(graph)

    assert len(query_runner.queries) == 1
    assert "gds.closeness.stats" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert "jobId" in params["config"]

    assert isinstance(result_obj, ClosenessStatsResult)
    assert result_obj.pre_processing_millis == 10
    assert result_obj.compute_millis == 20
    assert result_obj.post_processing_millis == 12
    assert result_obj.centrality_distribution == {"foo": 42}
    assert result_obj.configuration == {"bar": 1337}


def test_stream(
    closeness_endpoints: ClosenessCypherEndpoints, graph: Graph, query_runner: CollectingQueryRunner
) -> None:
    closeness_endpoints.stream(graph)

    assert len(query_runner.queries) == 1
    assert "gds.closeness.stream" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert "jobId" in config


def test_write(graph: Graph) -> None:
    result = {
        "nodePropertiesWritten": 5,
        "writeMillis": 42,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "centralityDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"closeness.write": pd.DataFrame([result])})

    result_obj = ClosenessCypherEndpoints(query_runner).write(graph, "closeness")

    assert len(query_runner.queries) == 1
    assert "gds.closeness.write" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["writeProperty"] == "closeness"
    assert "jobId" in config

    assert isinstance(result_obj, ClosenessWriteResult)
    assert result_obj.node_properties_written == 5
    assert result_obj.write_millis == 42


def test_estimate_with_graph_name(graph: Graph) -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"closeness.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    ClosenessCypherEndpoints(query_runner).estimate(graph)

    assert len(query_runner.queries) == 1
    assert "gds.closeness.stats.estimate" in query_runner.queries[0]


def test_estimate_with_projection_config() -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"closeness.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    projection_config = {
        "nodeProjection": "*",
        "relationshipProjection": "*",
    }

    ClosenessCypherEndpoints(query_runner).estimate(projection_config)

    assert len(query_runner.queries) == 1
    assert "gds.closeness.stats.estimate" in query_runner.queries[0]
