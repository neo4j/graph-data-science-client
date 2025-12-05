import pandas as pd
import pytest

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.centrality.closeness_harmonic_endpoints import (
    ClosenessHarmonicStatsResult,
    ClosenessHarmonicWriteResult,
)
from graphdatascience.procedure_surface.cypher.centrality.closeness_harmonic_cypher_endpoints import (
    ClosenessHarmonicCypherEndpoints,
)
from tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner
from tests.unit.procedure_surface.cypher.conftest import estimate_mock_result


@pytest.fixture
def closeness_harmonic_endpoints(query_runner: CollectingQueryRunner) -> ClosenessHarmonicCypherEndpoints:
    return ClosenessHarmonicCypherEndpoints(query_runner)


def test_mutate(graph: GraphV2, query_runner: CollectingQueryRunner) -> None:
    result = {
        "nodePropertiesWritten": 5,
        "mutateMillis": 42,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "centralityDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }
    query_runner.add__mock_result("closeness.harmonic.mutate", pd.DataFrame([result]))

    ClosenessHarmonicCypherEndpoints(query_runner).mutate(
        graph,
        "harmonic_closeness",
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
    )

    assert len(query_runner.queries) == 1
    assert "gds.closeness.harmonic.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "mutateProperty": "harmonic_closeness",
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
    }


def test_stats(graph: GraphV2) -> None:
    result = {
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "centralityDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"closeness.harmonic.stats": pd.DataFrame([result])})

    result_obj = ClosenessHarmonicCypherEndpoints(query_runner).stats(graph)

    assert len(query_runner.queries) == 1
    assert "gds.closeness.harmonic.stats" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert "jobId" in params["config"]

    assert isinstance(result_obj, ClosenessHarmonicStatsResult)
    assert result_obj.pre_processing_millis == 10
    assert result_obj.compute_millis == 20
    assert result_obj.post_processing_millis == 12
    assert result_obj.centrality_distribution == {"foo": 42}
    assert result_obj.configuration == {"bar": 1337}


def test_stream(
    closeness_harmonic_endpoints: ClosenessHarmonicCypherEndpoints, graph: GraphV2, query_runner: CollectingQueryRunner
) -> None:
    closeness_harmonic_endpoints.stream(graph)

    assert len(query_runner.queries) == 1
    assert "gds.closeness.harmonic.stream" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert "jobId" in config


def test_write(graph: GraphV2) -> None:
    result = {
        "nodePropertiesWritten": 5,
        "writeMillis": 42,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "centralityDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"closeness.harmonic.write": pd.DataFrame([result])})

    result_obj = ClosenessHarmonicCypherEndpoints(query_runner).write(graph, "harmonic_closeness")

    assert len(query_runner.queries) == 1
    assert "gds.closeness.harmonic.write" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["writeProperty"] == "harmonic_closeness"
    assert "jobId" in config

    assert isinstance(result_obj, ClosenessHarmonicWriteResult)
    assert result_obj.node_properties_written == 5
    assert result_obj.write_millis == 42


def test_estimate_with_graph_name(graph: GraphV2) -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"closeness.harmonic.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    ClosenessHarmonicCypherEndpoints(query_runner).estimate(graph)

    assert len(query_runner.queries) == 1
    assert "gds.closeness.harmonic.stats.estimate" in query_runner.queries[0]


def test_estimate_with_projection_config() -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"closeness.harmonic.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    projection_config = {
        "nodeProjection": "*",
        "relationshipProjection": "*",
    }

    ClosenessHarmonicCypherEndpoints(query_runner).estimate(projection_config)

    assert len(query_runner.queries) == 1
    assert "gds.closeness.harmonic.stats.estimate" in query_runner.queries[0]
