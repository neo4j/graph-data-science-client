from typing import Generator

import pytest

from graphdatascience import Graph, QueryRunner
from graphdatascience.procedure_surface.cypher.graphsage_cypher_endpoints import GraphSageCypherEndpoints


@pytest.fixture
def sample_graph_with_features(query_runner: QueryRunner) -> Generator[Graph, None, None]:
    create_statement = """
    CREATE
    (a: Node {feature: 1.0}),
    (b: Node {feature: 2.0}),
    (c: Node {feature: 3.0}),
    (a)-[:REL]->(c),
    (b)-[:REL]->(c)
    """

    query_runner.run_cypher(create_statement)

    query_runner.run_cypher("""
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {nodeProperties: 'feature'}) AS G
        RETURN G
    """)

    yield Graph("g", query_runner)

    query_runner.run_cypher("CALL gds.graph.drop('g')")
    query_runner.run_cypher("MATCH (n) DETACH DELETE n")


@pytest.fixture
def graphsage_endpoints(query_runner: QueryRunner) -> Generator[GraphSageCypherEndpoints, None, None]:
    yield GraphSageCypherEndpoints(query_runner)


def test_graphsage_train(graphsage_endpoints: GraphSageCypherEndpoints, sample_graph_with_features: Graph) -> None:
    """Test GraphSage train operation."""
    model, train_result = graphsage_endpoints.train(
        G=sample_graph_with_features,
        model_name="testModel",
        feature_properties=["feature"],
        embedding_dimension=64,
    )

    assert train_result.train_millis >= 0
    assert train_result.model_info is not None
    assert train_result.configuration is not None
    assert model.name() == "testModel"
