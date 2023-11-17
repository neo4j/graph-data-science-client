import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.model.simple_rel_embedding_model import SimpleRelEmbeddingModel
from graphdatascience.tests.unit.conftest import CollectingQueryRunner

GRAPH_NAME = "g"
NODE_PROP = "dummy_prop"
REL_TYPE = "dummy_type"
REL_TYPE_EMBEDDING = [1.0, 2.0, 3.0]
WRITE_MUTATE_REL_TYPE = "another_dummy_type"
WRITE_MUTATE_PROPERTY = "another_dummy_prop"
SOURCE_NODE_FILTER = "dummy_source_spec"
TARGET_NODE_FILTER = "dummy_target_spec"
TOP_K = 10
CONCURRENCY = 10


@pytest.fixture
def G(gds: GraphDataScience) -> Graph:
    G_, _ = gds.graph.project(GRAPH_NAME, "Node", "REL")
    return G_


@pytest.fixture
def transe_M(gds: GraphDataScience, G: Graph) -> SimpleRelEmbeddingModel:
    return gds.model.transe.create(G, NODE_PROP, {REL_TYPE: REL_TYPE_EMBEDDING})


@pytest.fixture
def distmult_M(gds: GraphDataScience, G: Graph) -> SimpleRelEmbeddingModel:
    return gds.model.distmult.create(G, NODE_PROP, {REL_TYPE: REL_TYPE_EMBEDDING})


def test_transe_predict_stream(runner: CollectingQueryRunner, transe_M: SimpleRelEmbeddingModel) -> None:
    _ = transe_M.predict_stream(SOURCE_NODE_FILTER, TARGET_NODE_FILTER, REL_TYPE, TOP_K, concurrency=CONCURRENCY)

    assert runner.last_query() == "CALL gds.ml.kge.predict.stream($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {
            "sourceNodeFilter": SOURCE_NODE_FILTER,
            "targetNodeFilter": TARGET_NODE_FILTER,
            "nodeEmbeddingProperty": NODE_PROP,
            "relationshipTypeEmbedding": REL_TYPE_EMBEDDING,
            "scoringFunction": "transe",
            "topK": TOP_K,
            "concurrency": CONCURRENCY,
        },
    }


def test_distmult_predict_stream(runner: CollectingQueryRunner, distmult_M: SimpleRelEmbeddingModel) -> None:
    _ = distmult_M.predict_stream(SOURCE_NODE_FILTER, TARGET_NODE_FILTER, REL_TYPE, TOP_K)

    assert runner.last_query() == "CALL gds.ml.kge.predict.stream($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {
            "sourceNodeFilter": SOURCE_NODE_FILTER,
            "targetNodeFilter": TARGET_NODE_FILTER,
            "nodeEmbeddingProperty": NODE_PROP,
            "relationshipTypeEmbedding": REL_TYPE_EMBEDDING,
            "scoringFunction": "distmult",
            "topK": TOP_K,
        },
    }


def test_transe_predict_mutate(runner: CollectingQueryRunner, transe_M: SimpleRelEmbeddingModel) -> None:
    _ = transe_M.predict_mutate(
        SOURCE_NODE_FILTER, TARGET_NODE_FILTER, REL_TYPE, TOP_K, WRITE_MUTATE_REL_TYPE, WRITE_MUTATE_PROPERTY
    )

    assert runner.last_query() == "CALL gds.ml.kge.predict.mutate($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {
            "sourceNodeFilter": SOURCE_NODE_FILTER,
            "targetNodeFilter": TARGET_NODE_FILTER,
            "nodeEmbeddingProperty": NODE_PROP,
            "relationshipTypeEmbedding": REL_TYPE_EMBEDDING,
            "scoringFunction": "transe",
            "topK": TOP_K,
            "mutateProperty": WRITE_MUTATE_PROPERTY,
            "mutateRelationshipType": WRITE_MUTATE_REL_TYPE,
        },
    }


def test_distmult_predict_mutate(runner: CollectingQueryRunner, distmult_M: SimpleRelEmbeddingModel) -> None:
    _ = distmult_M.predict_mutate(
        SOURCE_NODE_FILTER,
        TARGET_NODE_FILTER,
        REL_TYPE,
        TOP_K,
        WRITE_MUTATE_REL_TYPE,
        WRITE_MUTATE_PROPERTY,
        concurrency=CONCURRENCY,
    )

    assert runner.last_query() == "CALL gds.ml.kge.predict.mutate($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {
            "sourceNodeFilter": SOURCE_NODE_FILTER,
            "targetNodeFilter": TARGET_NODE_FILTER,
            "nodeEmbeddingProperty": NODE_PROP,
            "relationshipTypeEmbedding": REL_TYPE_EMBEDDING,
            "scoringFunction": "distmult",
            "topK": TOP_K,
            "mutateProperty": WRITE_MUTATE_PROPERTY,
            "mutateRelationshipType": WRITE_MUTATE_REL_TYPE,
            "concurrency": CONCURRENCY,
        },
    }


def test_transe_predict_write(runner: CollectingQueryRunner, transe_M: SimpleRelEmbeddingModel) -> None:
    _ = transe_M.predict_write(
        SOURCE_NODE_FILTER,
        TARGET_NODE_FILTER,
        REL_TYPE,
        TOP_K,
        WRITE_MUTATE_REL_TYPE,
        WRITE_MUTATE_PROPERTY,
        concurrency=CONCURRENCY,
    )

    assert runner.last_query() == "CALL gds.ml.kge.predict.write($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {
            "sourceNodeFilter": SOURCE_NODE_FILTER,
            "targetNodeFilter": TARGET_NODE_FILTER,
            "nodeEmbeddingProperty": NODE_PROP,
            "relationshipTypeEmbedding": REL_TYPE_EMBEDDING,
            "scoringFunction": "transe",
            "topK": TOP_K,
            "writeProperty": WRITE_MUTATE_PROPERTY,
            "writeRelationshipType": WRITE_MUTATE_REL_TYPE,
            "concurrency": CONCURRENCY,
        },
    }


def test_distmult_predict_write(runner: CollectingQueryRunner, distmult_M: SimpleRelEmbeddingModel) -> None:
    _ = distmult_M.predict_write(
        SOURCE_NODE_FILTER, TARGET_NODE_FILTER, REL_TYPE, TOP_K, WRITE_MUTATE_REL_TYPE, WRITE_MUTATE_PROPERTY
    )

    assert runner.last_query() == "CALL gds.ml.kge.predict.write($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {
            "sourceNodeFilter": SOURCE_NODE_FILTER,
            "targetNodeFilter": TARGET_NODE_FILTER,
            "nodeEmbeddingProperty": NODE_PROP,
            "relationshipTypeEmbedding": REL_TYPE_EMBEDDING,
            "scoringFunction": "distmult",
            "topK": TOP_K,
            "writeRelationshipType": WRITE_MUTATE_REL_TYPE,
            "writeProperty": WRITE_MUTATE_PROPERTY,
        },
    }


def test_transe_getters(transe_M: SimpleRelEmbeddingModel) -> None:
    assert transe_M.scoring_function() == "transe"
    assert transe_M.graph_name() == GRAPH_NAME
    assert transe_M.node_embedding_property() == NODE_PROP
    assert transe_M.relationship_type_embeddings() == {REL_TYPE: REL_TYPE_EMBEDDING}


def test_distmult_getters(distmult_M: SimpleRelEmbeddingModel) -> None:
    assert distmult_M.scoring_function() == "distmult"
    assert distmult_M.graph_name() == GRAPH_NAME
    assert distmult_M.node_embedding_property() == NODE_PROP
    assert distmult_M.relationship_type_embeddings() == {REL_TYPE: REL_TYPE_EMBEDDING}
