import pandas as pd
import pytest

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.centrality.degree_endpoints import (
    DegreeMutateResult,
    DegreeStatsResult,
    DegreeWriteResult,
)
from graphdatascience.procedure_surface.cypher.centrality.degree_cypher_endpoints import DegreeCypherEndpoints
from tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner
from tests.unit.procedure_surface.cypher.conftest import estimate_mock_result


@pytest.fixture
def degree_endpoints(query_runner: CollectingQueryRunner) -> DegreeCypherEndpoints:
    return DegreeCypherEndpoints(query_runner)


def test_mutate_basic(graph: GraphV2) -> None:
    result = {
        "nodePropertiesWritten": 5,
        "mutateMillis": 42,
        "centralityDistribution": {"min": 1.0, "max": 5.0, "mean": 2.5},
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "configuration": {"mutateProperty": "degree"},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"degree.mutate": pd.DataFrame([result])})

    result_obj = DegreeCypherEndpoints(query_runner).mutate(graph, "degree")

    assert len(query_runner.queries) == 1
    assert "gds.degree.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["mutateProperty"] == "degree"
    assert "jobId" in config

    assert isinstance(result_obj, DegreeMutateResult)
    assert result_obj.node_properties_written == 5
    assert result_obj.mutate_millis == 42


def test_mutate_with_optional_params(graph: GraphV2) -> None:
    result = {
        "nodePropertiesWritten": 3,
        "mutateMillis": 25,
        "centralityDistribution": {"min": 1.0, "max": 3.0, "mean": 2.0},
        "preProcessingMillis": 5,
        "computeMillis": 15,
        "postProcessingMillis": 5,
        "configuration": {"mutateProperty": "degree", "orientation": "UNDIRECTED"},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"degree.mutate": pd.DataFrame([result])})

    result_obj = DegreeCypherEndpoints(query_runner).mutate(
        graph,
        "degree",
        orientation="UNDIRECTED",
        relationship_types=["KNOWS"],
        node_labels=["Person"],
        concurrency=4,
        log_progress=True,
        relationship_weight_property="weight",
    )

    params = query_runner.params[0]
    config = params["config"]
    assert config["mutateProperty"] == "degree"
    assert config["orientation"] == "UNDIRECTED"
    assert config["relationshipTypes"] == ["KNOWS"]
    assert config["nodeLabels"] == ["Person"]
    assert config["concurrency"] == 4
    assert config["logProgress"] is True
    assert config["relationshipWeightProperty"] == "weight"

    assert isinstance(result_obj, DegreeMutateResult)
    assert result_obj.node_properties_written == 3
    assert result_obj.mutate_millis == 25


def test_stats_basic(graph: GraphV2) -> None:
    result = {
        "centralityDistribution": {"min": 1.0, "max": 5.0, "mean": 2.5},
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "configuration": {"orientation": "NATURAL"},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"degree.stats": pd.DataFrame([result])})

    result_obj = DegreeCypherEndpoints(query_runner).stats(graph)

    assert len(query_runner.queries) == 1
    assert "gds.degree.stats" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"

    assert isinstance(result_obj, DegreeStatsResult)
    assert result_obj.centrality_distribution == {"min": 1.0, "max": 5.0, "mean": 2.5}
    assert result_obj.pre_processing_millis == 10
    assert result_obj.compute_millis == 20
    assert result_obj.post_processing_millis == 12


def test_stream_basic(graph: GraphV2) -> None:
    result_df = pd.DataFrame({"nodeId": [0, 1, 2], "score": [2.0, 3.0, 1.0]})

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"degree.stream": result_df})

    result = DegreeCypherEndpoints(query_runner).stream(graph)

    assert len(query_runner.queries) == 1
    assert "gds.degree.stream" in query_runner.queries[0]

    assert result.equals(result_df)


def test_stream_with_optional_params(graph: GraphV2) -> None:
    result_df = pd.DataFrame({"nodeId": [0, 1], "score": [4.0, 2.0]})

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"degree.stream": result_df})

    result = DegreeCypherEndpoints(query_runner).stream(
        graph,
        orientation="UNDIRECTED",
        relationship_types=["CONNECTED"],
        node_labels=["Node"],
        concurrency=2,
        relationship_weight_property="strength",
    )

    params = query_runner.params[0]
    config = params["config"]
    assert config["orientation"] == "UNDIRECTED"
    assert config["relationshipTypes"] == ["CONNECTED"]
    assert config["nodeLabels"] == ["Node"]
    assert config["concurrency"] == 2
    assert config["relationshipWeightProperty"] == "strength"

    assert len(result) == 2


def test_write_basic(graph: GraphV2) -> None:
    result = {
        "nodePropertiesWritten": 5,
        "writeMillis": 42,
        "centralityDistribution": {"min": 1.0, "max": 5.0, "mean": 2.5},
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "configuration": {"writeProperty": "degree"},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"degree.write": pd.DataFrame([result])})

    result_obj = DegreeCypherEndpoints(query_runner).write(graph, "degree")

    assert len(query_runner.queries) == 1
    assert "gds.degree.write" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["writeProperty"] == "degree"

    assert isinstance(result_obj, DegreeWriteResult)
    assert result_obj.node_properties_written == 5
    assert result_obj.write_millis == 42


def test_write_with_optional_params(graph: GraphV2) -> None:
    result = {
        "nodePropertiesWritten": 3,
        "writeMillis": 25,
        "centralityDistribution": {"min": 1.0, "max": 3.0, "mean": 2.0},
        "preProcessingMillis": 5,
        "computeMillis": 15,
        "postProcessingMillis": 5,
        "configuration": {"writeProperty": "degree", "orientation": "REVERSE"},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"degree.write": pd.DataFrame([result])})

    result_obj = DegreeCypherEndpoints(query_runner).write(
        graph,
        "degree",
        orientation="REVERSE",
        write_concurrency=8,
        sudo=True,
        username="test_user",
    )

    params = query_runner.params[0]
    config = params["config"]
    assert config["writeProperty"] == "degree"
    assert config["orientation"] == "REVERSE"
    assert config["writeConcurrency"] == 8
    assert config["sudo"] is True
    assert config["username"] == "test_user"

    assert isinstance(result_obj, DegreeWriteResult)
    assert result_obj.node_properties_written == 3
    assert result_obj.write_millis == 25


def test_estimate(graph: GraphV2) -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"gds.degree.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    estimate_result = DegreeCypherEndpoints(query_runner).estimate(graph)

    assert len(query_runner.queries) == 1
    assert "gds.degree.stats.estimate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graphNameOrConfiguration"] == "test_graph"
    assert estimate_result.node_count == 100
