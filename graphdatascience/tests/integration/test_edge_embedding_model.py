from typing import Generator

import pytest

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.model.simple_rel_embedding_model import SimpleRelEmbeddingModel
from graphdatascience.server_version.server_version import ServerVersion

GRAPH_NAME = "g"

NODE_PROP = "z"
REL_TYPE = "REL"
REL_TYPE_EMBEDDING = [1.0, 2.0, 3.0]
WRITE_MUTATE_REL_TYPE = "another_dummy_type"
WRITE_MUTATE_PROPERTY = "another_dummy_prop"
SOURCE_NODE_FILTER = "Node"
TARGET_NODE_FILTER = "Node2"
TOP_K = 10
CONCURRENCY = 2


@pytest.fixture(autouse=True)
def run_around_tests(gds: GraphDataScience) -> Generator[None, None, None]:
    # Runs before each test
    gds.run_cypher(
        """
        CREATE
        (a: Node {x: 1, y: 2, z: [42.1, 131.0, 12.99]}),
        (b: Node {x: 2, y: 3, z: [1337.7, 1231.2, 12312.1]}),
        (c: Node2 {x: 3, y: 4, z: [9.11, 1.11, 1.41]}),
        (a)-[:REL {relX: 4, relY: 5}]->(b),
        (a)-[:REL {relX: 5, relY: 6}]->(c),
        (b)-[:REL {relX: 6, relY: 7}]->(c),
        (b)-[:REL2]->(c)
        """
    )

    yield  # Test runs here

    # Runs after each test
    gds.run_cypher("MATCH (n) DETACH DELETE n")
    gds.graph.drop(GRAPH_NAME)


@pytest.fixture
def transe_M(gds: GraphDataScience) -> Generator[SimpleRelEmbeddingModel, None, None]:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "z"}, "Node2": {"properties": "z"}}, "REL")

    yield gds.model.transe.create(G, NODE_PROP, {REL_TYPE: REL_TYPE_EMBEDDING})

    G.drop()


@pytest.fixture
def distmult_M(gds: GraphDataScience) -> Generator[SimpleRelEmbeddingModel, None, None]:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "z"}, "Node2": {"properties": "z"}}, "REL")

    yield gds.model.distmult.create(G, NODE_PROP, {REL_TYPE: REL_TYPE_EMBEDDING})

    G.drop()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_transe_predict_stream(transe_M: SimpleRelEmbeddingModel) -> None:
    result = transe_M.predict_stream(SOURCE_NODE_FILTER, TARGET_NODE_FILTER, REL_TYPE, TOP_K)
    assert result.shape[0] == 2


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_distmult_predict_stream(distmult_M: SimpleRelEmbeddingModel) -> None:
    result = distmult_M.predict_stream(SOURCE_NODE_FILTER, TARGET_NODE_FILTER, REL_TYPE, TOP_K, concurrency=CONCURRENCY)
    assert result.shape[0] == 2


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_transe_predict_mutate(transe_M: SimpleRelEmbeddingModel) -> None:
    result = transe_M.predict_mutate(
        SOURCE_NODE_FILTER,
        TARGET_NODE_FILTER,
        REL_TYPE,
        TOP_K,
        WRITE_MUTATE_REL_TYPE,
        WRITE_MUTATE_PROPERTY,
        concurrency=CONCURRENCY,
    )
    assert result["relationshipsWritten"] == 2


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_distmult_predict_mutate(distmult_M: SimpleRelEmbeddingModel) -> None:
    result = distmult_M.predict_mutate(
        SOURCE_NODE_FILTER, TARGET_NODE_FILTER, REL_TYPE, TOP_K, WRITE_MUTATE_REL_TYPE, WRITE_MUTATE_PROPERTY
    )
    assert result["relationshipsWritten"] == 2


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_transe_predict_write(transe_M: SimpleRelEmbeddingModel) -> None:
    result = transe_M.predict_write(
        SOURCE_NODE_FILTER, TARGET_NODE_FILTER, REL_TYPE, TOP_K, WRITE_MUTATE_REL_TYPE, WRITE_MUTATE_PROPERTY
    )
    assert result["relationshipsWritten"] == 2


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_distmult_predict_write(distmult_M: SimpleRelEmbeddingModel) -> None:
    result = distmult_M.predict_write(
        SOURCE_NODE_FILTER,
        TARGET_NODE_FILTER,
        REL_TYPE,
        TOP_K,
        WRITE_MUTATE_REL_TYPE,
        WRITE_MUTATE_PROPERTY,
        concurrency=CONCURRENCY,
    )
    assert result["relationshipsWritten"] == 2
