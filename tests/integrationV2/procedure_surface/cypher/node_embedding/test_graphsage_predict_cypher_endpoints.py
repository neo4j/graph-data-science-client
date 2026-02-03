from typing import Generator

import pytest

from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.model.graphsage_model import GraphSageModelV2
from graphdatascience.procedure_surface.cypher.node_embedding.graphsage_train_cypher_endpoints import (
    GraphSageTrainCypherEndpoints,
)
from graphdatascience.query_runner.query_runner import QueryRunner
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: Node {feature: 1.0}),
    (b: Node {feature: 2.0}),
    (c: Node {feature: 3.0}),
    (a)-[:REL]->(c),
    (b)-[:REL]->(c)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {sourceNodeProperties: properties(n), targetNodeProperties: properties(m)}) AS G
        RETURN G
    """

    with create_graph(
        query_runner,
        "g",
        create_statement,
        projection_query,
    ) as g:
        yield g


@pytest.fixture
def gs_model(query_runner: QueryRunner, sample_graph: GraphV2) -> Generator[GraphSageModelV2, None, None]:
    model, _ = GraphSageTrainCypherEndpoints(query_runner)(
        G=sample_graph,
        model_name="gs-model",
        feature_properties=["feature"],
        embedding_dimension=1,
        sample_sizes=[1],
        max_iterations=1,
        search_depth=1,
    )

    yield model

    query_runner.run_cypher("CALL gds.model.drop('gs-model')")


def test_stream(gs_model: GraphSageModelV2, sample_graph: GraphV2) -> None:
    result = gs_model.predict_stream(sample_graph, concurrency=4)

    assert set(result.columns) == {"nodeId", "embedding"}
    assert len(result) == 3


def test_mutate(gs_model: GraphSageModelV2, sample_graph: GraphV2) -> None:
    result = gs_model.predict_mutate(sample_graph, concurrency=4, mutate_property="embedding")

    assert result.node_properties_written == 3


def test_write(gs_model: GraphSageModelV2, sample_graph: GraphV2) -> None:
    result = gs_model.predict_write(sample_graph, write_property="embedding", concurrency=4, write_concurrency=2)

    assert result.node_properties_written == 3


def test_estimate(gs_model: GraphSageModelV2, sample_graph: GraphV2) -> None:
    result = gs_model.predict_estimate(sample_graph, concurrency=4)

    assert result.node_count == 3
    assert result.relationship_count == 2
    assert "KiB" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
