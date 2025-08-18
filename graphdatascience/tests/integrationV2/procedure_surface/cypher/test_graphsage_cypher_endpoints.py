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
    result = graphsage_endpoints.train(
        G=sample_graph_with_features,
        model_name="testModel",
        feature_properties=["feature"],
        embedding_dimension=64,
    )

    assert result.train_millis >= 0
    assert result.model_info is not None
    assert result.configuration is not None
    assert "testModel" in str(result.model_info)


def test_graphsage_mutate(graphsage_endpoints: GraphSageCypherEndpoints, sample_graph_with_features: Graph) -> None:
    """Test GraphSage mutate operation."""
    # First train a model
    graphsage_endpoints.train(
        G=sample_graph_with_features,
        model_name="testMutateModel",
        feature_properties=["feature"],
        embedding_dimension=64,
    )

    # Then use it for mutate
    result = graphsage_endpoints.mutate(
        G=sample_graph_with_features,
        model_name="testMutateModel",
        mutate_property="graphsage_embedding",
    )

    assert result.node_count == 3
    assert result.node_properties_written == 3
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.configuration is not None


def test_graphsage_stream(graphsage_endpoints: GraphSageCypherEndpoints, sample_graph_with_features: Graph) -> None:
    """Test GraphSage stream operation."""
    # First train a model
    graphsage_endpoints.train(
        G=sample_graph_with_features,
        model_name="testStreamModel",
        feature_properties=["feature"],
        embedding_dimension=64,
    )

    # Then use it for stream
    result = graphsage_endpoints.stream(
        G=sample_graph_with_features,
        model_name="testStreamModel",
    )

    assert len(result) == 3  # We have 3 nodes

    # Check that we have the expected result structure
    # For Cypher endpoints, this returns a DataFrame with string columns
    assert "nodeId" in result.columns
    assert "embedding" in result.columns


def test_graphsage_write(graphsage_endpoints: GraphSageCypherEndpoints, sample_graph_with_features: Graph) -> None:
    """Test GraphSage write operation."""
    # First train a model
    graphsage_endpoints.train(
        G=sample_graph_with_features,
        model_name="testWriteModel",
        feature_properties=["feature"],
        embedding_dimension=64,
    )

    # Then use it for write
    result = graphsage_endpoints.write(
        G=sample_graph_with_features,
        model_name="testWriteModel",
        write_property="graphsage_embedding",
    )

    assert result.node_count == 3
    assert result.node_properties_written == 3
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.configuration is not None
