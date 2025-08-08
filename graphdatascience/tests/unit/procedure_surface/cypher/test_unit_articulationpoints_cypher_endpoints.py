import pandas as pd
import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.procedure_surface.api.articulationpoints_endpoints import (
    ArticulationPointsMutateResult,
    ArticulationPointsStatsResult,
    ArticulationPointsWriteResult,
)
from graphdatascience.procedure_surface.cypher.articulationpoints_cypher_endpoints import (
    ArticulationPointsCypherEndpoints,
)
from graphdatascience.tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner
from graphdatascience.tests.unit.procedure_surface.cypher.conftests import estimate_mock_result


@pytest.fixture
def query_runner() -> CollectingQueryRunner:
    return CollectingQueryRunner(DEFAULT_SERVER_VERSION)


@pytest.fixture
def articulationpoints_endpoints(query_runner: CollectingQueryRunner) -> ArticulationPointsCypherEndpoints:
    return ArticulationPointsCypherEndpoints(query_runner)


@pytest.fixture
def graph(query_runner: CollectingQueryRunner) -> Graph:
    return Graph("test_graph", query_runner)


def test_mutate_basic(articulationpoints_endpoints: ArticulationPointsCypherEndpoints, graph: Graph) -> None:
    result = {
        "articulationPointCount": 2,
        "computeMillis": 20,
        "mutateMillis": 15,
        "nodePropertiesWritten": 2,
        "configuration": {"mutateProperty": "articulationPoint"},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"articulationPoints.mutate": pd.DataFrame([result])})

    result_obj = ArticulationPointsCypherEndpoints(query_runner).mutate(graph, "articulationPoint")

    assert len(query_runner.queries) == 1
    assert "gds.articulationPoints.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["mutateProperty"] == "articulationPoint"
    assert "jobId" in config

    assert isinstance(result_obj, ArticulationPointsMutateResult)
    assert result_obj.articulation_point_count == 2
    assert result_obj.compute_millis == 20
    assert result_obj.mutate_millis == 15
    assert result_obj.node_properties_written == 2
    assert result_obj.configuration == {"mutateProperty": "articulationPoint"}


def test_mutate_with_optional_params(
    articulationpoints_endpoints: ArticulationPointsCypherEndpoints, graph: Graph
) -> None:
    result = {
        "articulationPointCount": 3,
        "computeMillis": 25,
        "mutateMillis": 20,
        "nodePropertiesWritten": 3,
        "configuration": {"mutateProperty": "articulationPoint", "concurrency": 4},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"articulationPoints.mutate": pd.DataFrame([result])})

    ArticulationPointsCypherEndpoints(query_runner).mutate(
        graph,
        "articulationPoint",
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
    )

    assert len(query_runner.queries) == 1
    assert "gds.articulationPoints.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "mutateProperty": "articulationPoint",
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
    }


def test_stats_basic(articulationpoints_endpoints: ArticulationPointsCypherEndpoints, graph: Graph) -> None:
    result = {
        "articulationPointCount": 1,
        "computeMillis": 15,
        "configuration": {"concurrency": 1},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"articulationPoints.stats": pd.DataFrame([result])})

    result_obj = ArticulationPointsCypherEndpoints(query_runner).stats(graph)

    assert len(query_runner.queries) == 1
    assert "gds.articulationPoints.stats" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert "jobId" in config

    assert isinstance(result_obj, ArticulationPointsStatsResult)
    assert result_obj.articulation_point_count == 1
    assert result_obj.compute_millis == 15
    assert result_obj.configuration == {"concurrency": 1}


def test_stream_basic(articulationpoints_endpoints: ArticulationPointsCypherEndpoints, graph: Graph) -> None:
    result = pd.DataFrame(
        [
            {"nodeId": 0, "resultingComponents": {"max": 1}},
            {"nodeId": 2, "resultingComponents": {"max": 2}},
        ]
    )

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"articulationPoints.stream": result})

    result_df = ArticulationPointsCypherEndpoints(query_runner).stream(graph)

    assert len(query_runner.queries) == 1
    assert "gds.articulationPoints.stream" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert "jobId" in config

    assert len(result_df) == 2
    assert "nodeId" in result_df.columns
    assert "resultingComponents" in result_df.columns


def test_write_basic(articulationpoints_endpoints: ArticulationPointsCypherEndpoints, graph: Graph) -> None:
    result = {
        "articulationPointCount": 2,
        "computeMillis": 20,
        "writeMillis": 10,
        "nodePropertiesWritten": 2,
        "configuration": {"writeProperty": "articulationPoint"},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"articulationPoints.write": pd.DataFrame([result])})

    result_obj = ArticulationPointsCypherEndpoints(query_runner).write(graph, "articulationPoint")

    assert len(query_runner.queries) == 1
    assert "gds.articulationPoints.write" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["writeProperty"] == "articulationPoint"
    assert "jobId" in config

    assert isinstance(result_obj, ArticulationPointsWriteResult)
    assert result_obj.articulation_point_count == 2
    assert result_obj.compute_millis == 20
    assert result_obj.write_millis == 10
    assert result_obj.node_properties_written == 2
    assert result_obj.configuration == {"writeProperty": "articulationPoint"}


def test_write_with_optional_params(
    articulationpoints_endpoints: ArticulationPointsCypherEndpoints, graph: Graph
) -> None:
    result = {
        "articulationPointCount": 3,
        "computeMillis": 25,
        "writeMillis": 15,
        "nodePropertiesWritten": 3,
        "configuration": {"writeProperty": "articulationPoint", "writeConcurrency": 2},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"articulationPoints.write": pd.DataFrame([result])})

    ArticulationPointsCypherEndpoints(query_runner).write(
        graph,
        "articulationPoint",
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
        write_concurrency=2,
    )

    assert len(query_runner.queries) == 1
    assert "gds.articulationPoints.write" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "writeProperty": "articulationPoint",
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
        "writeConcurrency": 2,
    }


def test_estimate_with_graph_name(graph: Graph) -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"articulationPoints.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    ArticulationPointsCypherEndpoints(query_runner).estimate(graph)

    assert len(query_runner.queries) == 1
    assert "gds.articulationPoints.stats.estimate" in query_runner.queries[0]


def test_estimate_with_projection_config(query_runner: CollectingQueryRunner) -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"articulationPoints.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    projection_config = {
        "nodeProjection": "*",
        "relationshipProjection": "*",
    }

    ArticulationPointsCypherEndpoints(query_runner).estimate(projection_config, relationship_types=["REL"])

    assert len(query_runner.queries) == 1
    assert "gds.articulationPoints.stats.estimate" in query_runner.queries[0]
