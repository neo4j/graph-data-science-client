from typing import Generator

import pytest

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.kge.kge_endpoints import KgeEndpoints
from graphdatascience.procedure_surface.api.kge.simple_rel_embedding_model import SimpleRelEmbeddingModel
from graphdatascience.procedure_surface.cypher.kge.kge_predict_cypher_endpoints import KgePredictCypherEndpoints
from graphdatascience.query_runner import QueryRunner
from tests.integration.procedure_surface.cypher.cypher_graph_helper import create_graph

NODE_PROP = "z"
REL_TYPE = "REL"
REL_TYPE_EMBEDDING = [1.0, 2.0, 3.0]
SOURCE_NODE_FILTER = "Node"
TARGET_NODE_FILTER = "Node2"
TOP_K = 2


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[Graph, None, None]:
    create_statement = """
    CREATE
    (a: Node {z: [42.1, 131.0, 12.99]}),
    (b: Node {z: [1337.7, 1231.2, 12312.1]}),
    (c: Node2 {z: [9.11, 1.11, 1.41]}),
    (d: Node2 {z: [0.1, 0.2, 0.3]}),
    (a)-[:REL]->(c)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project(
            'g', n, m,
            {sourceNodeLabels: labels(n), sourceNodeProperties: properties(n), targetNodeLabels: labels(m), targetNodeProperties: properties(m), relationshipType: type(r) }
        ) AS G
        RETURN G
    """

    with create_graph(query_runner, "g", create_statement, projection_query) as g:
        yield g


@pytest.fixture
def transe_model(query_runner: QueryRunner, sample_graph: Graph) -> SimpleRelEmbeddingModel:
    endpoints = KgeEndpoints(KgePredictCypherEndpoints(query_runner))
    return endpoints.transe(sample_graph, NODE_PROP, {REL_TYPE: REL_TYPE_EMBEDDING})


@pytest.fixture
def distmult_model(query_runner: QueryRunner, sample_graph: Graph) -> SimpleRelEmbeddingModel:
    endpoints = KgeEndpoints(KgePredictCypherEndpoints(query_runner))
    return endpoints.distmult(sample_graph, NODE_PROP, {REL_TYPE: REL_TYPE_EMBEDDING})


def test_transe_predict_stream(transe_model: SimpleRelEmbeddingModel) -> None:
    result = transe_model.predict_stream(SOURCE_NODE_FILTER, TARGET_NODE_FILTER, REL_TYPE, TOP_K)
    assert result.shape[0] > 0


def test_distmult_predict_stream(distmult_model: SimpleRelEmbeddingModel) -> None:
    result = distmult_model.predict_stream(SOURCE_NODE_FILTER, TARGET_NODE_FILTER, REL_TYPE, TOP_K)
    assert result.shape[0] > 0


def test_transe_predict_mutate(transe_model: SimpleRelEmbeddingModel) -> None:
    result = transe_model.predict_mutate(SOURCE_NODE_FILTER, TARGET_NODE_FILTER, REL_TYPE, TOP_K, "PREDICTED", "score")
    assert result.relationships_written >= 1


def test_transe_predict_write(transe_model: SimpleRelEmbeddingModel) -> None:
    result = transe_model.predict_write(SOURCE_NODE_FILTER, TARGET_NODE_FILTER, REL_TYPE, TOP_K, "PREDICTED", "score")
    assert result.relationships_written >= 1
