import pandas as pd
import pytest

from graphdatascience.graph.graph_api import Graph
from graphdatascience.graph.graph_backend_cypher import get_graph
from graphdatascience.procedure_surface.api.kge.kge_endpoints import KgeEndpoints
from graphdatascience.procedure_surface.api.kge.kge_predict_endpoints import KgeMutateResult, KgeWriteResult
from graphdatascience.procedure_surface.cypher.kge.kge_predict_cypher_endpoints import KgePredictCypherEndpoints
from tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner

NODE_PROP = "z"
REL_TYPE = "REL"
REL_TYPE_EMBEDDING = [1.0, 2.0, 3.0]
SOURCE_NODE_FILTER = "Node"
TARGET_NODE_FILTER = "Node2"
TOP_K = 10
CONCURRENCY = 2


@pytest.fixture
def graph() -> Graph:
    return get_graph("g", CollectingQueryRunner(DEFAULT_SERVER_VERSION))


def _kge(query_runner: CollectingQueryRunner) -> KgeEndpoints:
    return KgeEndpoints(KgePredictCypherEndpoints(query_runner))


def test_transe_predict_stream(graph: Graph) -> None:
    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION)
    model = _kge(query_runner).transe(graph, NODE_PROP, {REL_TYPE: REL_TYPE_EMBEDDING})

    model.predict_stream(SOURCE_NODE_FILTER, TARGET_NODE_FILTER, REL_TYPE, TOP_K, concurrency=CONCURRENCY)

    assert "gds.ml.kge.predict.stream" in query_runner.last_query()
    params = query_runner.last_params()
    assert params["graph_name"] == "g"
    config = params["config"]
    assert config["sourceNodeFilter"] == SOURCE_NODE_FILTER
    assert config["targetNodeFilter"] == TARGET_NODE_FILTER
    assert config["nodeEmbeddingProperty"] == NODE_PROP
    assert config["relationshipTypeEmbedding"] == REL_TYPE_EMBEDDING
    assert config["scoringFunction"] == "transe"
    assert config["topK"] == TOP_K
    assert config["concurrency"] == CONCURRENCY
    assert "jobId" in config


def test_flat_predict_stream(graph: Graph) -> None:
    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION)

    _kge(query_runner).predict.stream(
        graph,
        node_embedding_property=NODE_PROP,
        relationship_type_embedding=REL_TYPE_EMBEDDING,
        scoring_function="transe",
        top_k=TOP_K,
        source_node_filter=SOURCE_NODE_FILTER,
        target_node_filter=TARGET_NODE_FILTER,
    )

    assert "gds.ml.kge.predict.stream" in query_runner.last_query()
    config = query_runner.last_params()["config"]
    assert config["scoringFunction"] == "transe"
    assert config["nodeEmbeddingProperty"] == NODE_PROP
    assert config["topK"] == TOP_K


def test_distmult_predict_stream(graph: Graph) -> None:
    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION)
    model = _kge(query_runner).distmult(graph, NODE_PROP, {REL_TYPE: REL_TYPE_EMBEDDING})

    model.predict_stream(SOURCE_NODE_FILTER, TARGET_NODE_FILTER, REL_TYPE, TOP_K)

    assert "gds.ml.kge.predict.stream" in query_runner.last_query()
    assert query_runner.last_params()["config"]["scoringFunction"] == "distmult"


def test_predict_mutate(graph: Graph) -> None:
    result = {
        "relationshipsWritten": 2,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "mutateMillis": 15,
        "postProcessingMillis": 5,
        "configuration": {"bar": 1337},
    }
    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"gds.ml.kge.predict.mutate": pd.DataFrame([result])})
    model = _kge(query_runner).transe(graph, NODE_PROP, {REL_TYPE: REL_TYPE_EMBEDDING})

    result_obj = model.predict_mutate(
        SOURCE_NODE_FILTER, TARGET_NODE_FILTER, REL_TYPE, TOP_K, "PREDICTED", "score", concurrency=CONCURRENCY
    )

    assert isinstance(result_obj, KgeMutateResult)
    assert result_obj.relationships_written == 2
    config = query_runner.last_params()["config"]
    assert config["mutateRelationshipType"] == "PREDICTED"
    assert config["mutateProperty"] == "score"
    assert config["scoringFunction"] == "transe"


def test_predict_write(graph: Graph) -> None:
    result = {
        "relationshipsWritten": 2,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "writeMillis": 15,
        "configuration": {"bar": 1337},
    }
    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"gds.ml.kge.predict.write": pd.DataFrame([result])})
    model = _kge(query_runner).distmult(graph, NODE_PROP, {REL_TYPE: REL_TYPE_EMBEDDING})

    result_obj = model.predict_write(SOURCE_NODE_FILTER, TARGET_NODE_FILTER, REL_TYPE, TOP_K, "PREDICTED", "score")

    assert isinstance(result_obj, KgeWriteResult)
    assert result_obj.relationships_written == 2
    config = query_runner.last_params()["config"]
    assert config["writeRelationshipType"] == "PREDICTED"
    assert config["writeProperty"] == "score"
    assert config["scoringFunction"] == "distmult"
