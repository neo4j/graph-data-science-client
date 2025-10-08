from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.node_embedding.graphsage_train_cypher_endpoints import (
    GraphSageTrainCypherEndpoints,
)
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph_with_features(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
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
def graphsage_endpoints(query_runner: QueryRunner) -> Generator[GraphSageTrainCypherEndpoints, None, None]:
    yield GraphSageTrainCypherEndpoints(query_runner)


def test_graphsage_train(
    graphsage_endpoints: GraphSageTrainCypherEndpoints, sample_graph_with_features: GraphV2
) -> None:
    """Test GraphSage train operation."""
    model, train_result = graphsage_endpoints.train(
        G=sample_graph_with_features,
        model_name="testModel",
        feature_properties=["feature"],
        embedding_dimension=1,
        epochs=1,  # Use minimal epochs for faster testing
        max_iterations=1,  # Use minimal iterations for faster testing
    )

    assert train_result.train_millis >= 0
    assert train_result.model_info is not None
    assert train_result.configuration is not None
    assert model.name() == "testModel"
