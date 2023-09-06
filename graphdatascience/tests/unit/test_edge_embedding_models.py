import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.model.simple_edge_embedding_model import SimpleEdgeEmbeddingModel
from graphdatascience.tests.unit.conftest import CollectingQueryRunner

GRAPH_NAME = "g"
NODE_PROP = "dummy_prop"
REL_TYPE = "dummy_type"
REL_TYPE_EMBEDDING = [1.0, 2.0, 3.0]
WRITE_MUTATE_REL_TYPE = "another_dummy_type"
SOURCE_NODE_SPEC = "dummy_source_spec"
TARGET_NODE_SPEC = "dummy_target_spec"
TOP_K = 10
THRESHOLD = 0.5


@pytest.fixture
def G(gds: GraphDataScience) -> Graph:
    G_, _ = gds.graph.project(GRAPH_NAME, "Node", "REL")
    return G_


@pytest.fixture
def transe_M(gds: GraphDataScience, G: Graph) -> SimpleEdgeEmbeddingModel:
    return gds.model.transe.create(G, NODE_PROP, {REL_TYPE: REL_TYPE_EMBEDDING})


@pytest.fixture
def distmult_M(gds: GraphDataScience, G: Graph) -> SimpleEdgeEmbeddingModel:
    return gds.model.distmult.create(G, NODE_PROP, {REL_TYPE: REL_TYPE_EMBEDDING})


def test_transe_predict_stream(runner: CollectingQueryRunner, transe_M: SimpleEdgeEmbeddingModel) -> None:
    _ = transe_M.predict_stream(SOURCE_NODE_SPEC, TARGET_NODE_SPEC, REL_TYPE, TOP_K, THRESHOLD)

    assert runner.last_query() == (
        """
            CALL gds.ml.kge.predict.stream(
                $graph_name,
                {
                    sourceNodeSpec: $source_node_spec
                    targetNodeSpec: $target_node_spec
                    nodeEmbeddingProperty: $node_embedding_property
                    relationshipTypeEmbedding: $relationship_type_embedding
                    scoringFunction: $scoring_function,
                    topK: $top_k,
                    threshold: $threshold
                }
            )
            """
    )
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "source_node_spec": SOURCE_NODE_SPEC,
        "target_node_spec": TARGET_NODE_SPEC,
        "node_embedding_property": NODE_PROP,
        "relationship_type_embedding": REL_TYPE_EMBEDDING,
        "scoring_function": "transe",
        "top_k": TOP_K,
        "threshold": THRESHOLD,
    }


def test_distmult_predict_stream(runner: CollectingQueryRunner, distmult_M: SimpleEdgeEmbeddingModel) -> None:
    _ = distmult_M.predict_stream(SOURCE_NODE_SPEC, TARGET_NODE_SPEC, REL_TYPE, TOP_K, THRESHOLD)

    assert runner.last_query() == (
        """
            CALL gds.ml.kge.predict.stream(
                $graph_name,
                {
                    sourceNodeSpec: $source_node_spec
                    targetNodeSpec: $target_node_spec
                    nodeEmbeddingProperty: $node_embedding_property
                    relationshipTypeEmbedding: $relationship_type_embedding
                    scoringFunction: $scoring_function,
                    topK: $top_k,
                    threshold: $threshold
                }
            )
            """
    )
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "source_node_spec": SOURCE_NODE_SPEC,
        "target_node_spec": TARGET_NODE_SPEC,
        "node_embedding_property": NODE_PROP,
        "relationship_type_embedding": REL_TYPE_EMBEDDING,
        "scoring_function": "distmult",
        "top_k": TOP_K,
        "threshold": THRESHOLD,
    }


def test_transe_predict_mutate(runner: CollectingQueryRunner, transe_M: SimpleEdgeEmbeddingModel) -> None:
    _ = transe_M.predict_mutate(SOURCE_NODE_SPEC, TARGET_NODE_SPEC, REL_TYPE, TOP_K, THRESHOLD, WRITE_MUTATE_REL_TYPE)

    assert runner.last_query() == (
        """
            CALL gds.ml.kge.predict.mutate(
                $graph_name,
                {
                    sourceNodeSpec: $source_node_spec
                    targetNodeSpec: $target_node_spec
                    nodeEmbeddingProperty: $node_embedding_property
                    relationshipTypeEmbedding: $relationship_type_embedding
                    scoringFunction: $scoring_function,
                    topK: $top_k,
                    threshold: $threshold,
                    mutateRelationshipType: $mutate_relationship_type
                }
            )
            """
    )
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "source_node_spec": SOURCE_NODE_SPEC,
        "target_node_spec": TARGET_NODE_SPEC,
        "node_embedding_property": NODE_PROP,
        "relationship_type_embedding": REL_TYPE_EMBEDDING,
        "scoring_function": "transe",
        "top_k": TOP_K,
        "threshold": THRESHOLD,
        "mutate_relationship_type": WRITE_MUTATE_REL_TYPE,
    }


def test_distmult_predict_mutate(runner: CollectingQueryRunner, distmult_M: SimpleEdgeEmbeddingModel) -> None:
    _ = distmult_M.predict_mutate(SOURCE_NODE_SPEC, TARGET_NODE_SPEC, REL_TYPE, TOP_K, THRESHOLD, WRITE_MUTATE_REL_TYPE)

    assert runner.last_query() == (
        """
            CALL gds.ml.kge.predict.mutate(
                $graph_name,
                {
                    sourceNodeSpec: $source_node_spec
                    targetNodeSpec: $target_node_spec
                    nodeEmbeddingProperty: $node_embedding_property
                    relationshipTypeEmbedding: $relationship_type_embedding
                    scoringFunction: $scoring_function,
                    topK: $top_k,
                    threshold: $threshold,
                    mutateRelationshipType: $mutate_relationship_type
                }
            )
            """
    )
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "source_node_spec": SOURCE_NODE_SPEC,
        "target_node_spec": TARGET_NODE_SPEC,
        "node_embedding_property": NODE_PROP,
        "relationship_type_embedding": REL_TYPE_EMBEDDING,
        "scoring_function": "distmult",
        "top_k": TOP_K,
        "threshold": THRESHOLD,
        "mutate_relationship_type": WRITE_MUTATE_REL_TYPE,
    }


def test_transe_predict_write(runner: CollectingQueryRunner, transe_M: SimpleEdgeEmbeddingModel) -> None:
    _ = transe_M.predict_write(SOURCE_NODE_SPEC, TARGET_NODE_SPEC, REL_TYPE, TOP_K, THRESHOLD, WRITE_MUTATE_REL_TYPE)

    assert runner.last_query() == (
        """
            CALL gds.ml.kge.predict.mutate(
                $graph_name,
                {
                    sourceNodeSpec: $source_node_spec
                    targetNodeSpec: $target_node_spec
                    nodeEmbeddingProperty: $node_embedding_property
                    relationshipTypeEmbedding: $relationship_type_embedding
                    scoringFunction: $scoring_function,
                    topK: $top_k,
                    threshold: $threshold,
                    writeRelationshipType: $write_relationship_type
                }
            )
            """
    )
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "source_node_spec": SOURCE_NODE_SPEC,
        "target_node_spec": TARGET_NODE_SPEC,
        "node_embedding_property": NODE_PROP,
        "relationship_type_embedding": REL_TYPE_EMBEDDING,
        "scoring_function": "transe",
        "top_k": TOP_K,
        "threshold": THRESHOLD,
        "write_relationship_type": WRITE_MUTATE_REL_TYPE,
    }


def test_distmult_predict_write(runner: CollectingQueryRunner, distmult_M: SimpleEdgeEmbeddingModel) -> None:
    _ = distmult_M.predict_write(SOURCE_NODE_SPEC, TARGET_NODE_SPEC, REL_TYPE, TOP_K, THRESHOLD, WRITE_MUTATE_REL_TYPE)

    assert runner.last_query() == (
        """
            CALL gds.ml.kge.predict.mutate(
                $graph_name,
                {
                    sourceNodeSpec: $source_node_spec
                    targetNodeSpec: $target_node_spec
                    nodeEmbeddingProperty: $node_embedding_property
                    relationshipTypeEmbedding: $relationship_type_embedding
                    scoringFunction: $scoring_function,
                    topK: $top_k,
                    threshold: $threshold,
                    writeRelationshipType: $write_relationship_type
                }
            )
            """
    )
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "source_node_spec": SOURCE_NODE_SPEC,
        "target_node_spec": TARGET_NODE_SPEC,
        "node_embedding_property": NODE_PROP,
        "relationship_type_embedding": REL_TYPE_EMBEDDING,
        "scoring_function": "distmult",
        "top_k": TOP_K,
        "threshold": THRESHOLD,
        "write_relationship_type": WRITE_MUTATE_REL_TYPE,
    }
