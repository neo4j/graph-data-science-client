import pandas as pd
import pytest

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.eigenvector_endpoints import (
    EigenvectorMutateResult,
    EigenvectorStatsResult,
    EigenvectorWriteResult,
)
from graphdatascience.procedure_surface.cypher.eigenvector_cypher_endpoints import EigenvectorCypherEndpoints
from graphdatascience.tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner
from graphdatascience.tests.unit.procedure_surface.cypher.conftests import estimate_mock_result


@pytest.fixture
def eigenvector_endpoints(query_runner: CollectingQueryRunner) -> EigenvectorCypherEndpoints:
    return EigenvectorCypherEndpoints(query_runner)


def test_mutate(graph: GraphV2) -> None:
    result = {
        "nodePropertiesWritten": 5,
        "ranIterations": 10,
        "didConverge": True,
        "mutateMillis": 42,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "centralityDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"eigenvector.mutate": pd.DataFrame([result])})

    result_obj = EigenvectorCypherEndpoints(query_runner).mutate(
        graph,
        "eigenvector",
        max_iterations=20,
        tolerance=0.0001,
        source_nodes=[1, 2, 3],
        scaler={"type": "L2Norm"},
        relationship_weight_property="weight",
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
    )

    assert len(query_runner.queries) == 1
    assert "gds.eigenvector.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "mutateProperty": "eigenvector",
        "maxIterations": 20,
        "tolerance": 0.0001,
        "sourceNodes": [1, 2, 3],
        "scaler": {"type": "L2Norm"},
        "relationshipWeightProperty": "weight",
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
    }

    assert isinstance(result_obj, EigenvectorMutateResult)
    assert result_obj.node_properties_written == 5
    assert result_obj.ran_iterations == 10
    assert result_obj.did_converge is True
    assert result_obj.mutate_millis == 42
    assert result_obj.pre_processing_millis == 10
    assert result_obj.compute_millis == 20
    assert result_obj.post_processing_millis == 12
    assert result_obj.centrality_distribution == {"foo": 42}
    assert result_obj.configuration == {"bar": 1337}


def test_stats(graph: GraphV2) -> None:
    result = {
        "ranIterations": 10,
        "didConverge": True,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "centralityDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"eigenvector.stats": pd.DataFrame([result])})

    result_obj = EigenvectorCypherEndpoints(query_runner).stats(graph)

    assert len(query_runner.queries) == 1
    assert "gds.eigenvector.stats" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert "jobId" in params["config"]

    assert isinstance(result_obj, EigenvectorStatsResult)
    assert result_obj.ran_iterations == 10
    assert result_obj.did_converge is True
    assert result_obj.pre_processing_millis == 10
    assert result_obj.compute_millis == 20
    assert result_obj.post_processing_millis == 12
    assert result_obj.centrality_distribution == {"foo": 42}
    assert result_obj.configuration == {"bar": 1337}


def test_stream(
    eigenvector_endpoints: EigenvectorCypherEndpoints, graph: GraphV2, query_runner: CollectingQueryRunner
) -> None:
    eigenvector_endpoints.stream(graph)

    assert len(query_runner.queries) == 1
    assert "gds.eigenvector.stream" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert "jobId" in config


def test_write(graph: GraphV2) -> None:
    result = {
        "nodePropertiesWritten": 5,
        "ranIterations": 10,
        "didConverge": True,
        "writeMillis": 42,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "centralityDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"eigenvector.write": pd.DataFrame([result])})

    result_obj = EigenvectorCypherEndpoints(query_runner).write(graph, "eigenvector")

    assert len(query_runner.queries) == 1
    assert "gds.eigenvector.write" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["writeProperty"] == "eigenvector"
    assert "jobId" in config

    assert isinstance(result_obj, EigenvectorWriteResult)
    assert result_obj.node_properties_written == 5
    assert result_obj.ran_iterations == 10
    assert result_obj.did_converge is True
    assert result_obj.write_millis == 42


def test_estimate_with_graph_name(graph: GraphV2) -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"eigenvector.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    EigenvectorCypherEndpoints(query_runner).estimate(graph)

    assert len(query_runner.queries) == 1
    assert "gds.eigenvector.stats.estimate" in query_runner.queries[0]


def test_estimate_with_projection_config() -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"eigenvector.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    projection_config = {
        "nodeProjection": "*",
        "relationshipProjection": "*",
    }

    EigenvectorCypherEndpoints(query_runner).estimate(projection_config)

    assert len(query_runner.queries) == 1
    assert "gds.eigenvector.stats.estimate" in query_runner.queries[0]
