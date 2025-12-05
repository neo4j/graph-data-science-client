from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.node_embedding.graphsage_train_arrow_endpoints import (
    GraphSageTrainArrowEndpoints,
)
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    gdl = """
    CREATE
    (a: Node {feature: 1.0}),
    (b: Node {feature: 2.0}),
    (c: Node {feature: 3.0}),
    (d: Node {feature: 4.0}),
    (a)-[:REL]->(b),
    (b)-[:REL]->(c),
    (c)-[:REL]->(d),
    (d)-[:REL]->(a)
    """

    with create_graph(arrow_client, "g", gdl) as G:
        yield G


@pytest.fixture
def graphsage_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[GraphSageTrainArrowEndpoints, None, None]:
    yield GraphSageTrainArrowEndpoints(arrow_client, None)


def test_graphsage_train(graphsage_endpoints: GraphSageTrainArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test GraphSage train operation."""
    model, result = graphsage_endpoints(
        G=sample_graph,
        model_name="testGraphSageModel",
        feature_properties=["feature"],
        embedding_dimension=1,
        epochs=1,  # Use minimal epochs for faster testing
        max_iterations=1,  # Use minimal iterations for faster testing
    )

    # Check the result
    assert result.train_millis >= 0
    assert result.configuration is not None
    assert result.model_info is not None

    # Check the model
    assert model.name() == "testGraphSageModel"
    assert model.exists()

    # Clean up the model
    model.drop()


def test_graphsage_train_estimate(graphsage_endpoints: GraphSageTrainArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test GraphSage estimate operation."""
    result = graphsage_endpoints.estimate(
        G=sample_graph,
        model_name="testGraphSageModel",
        feature_properties=["feature"],
        embedding_dimension=1,
        epochs=1,
        max_iterations=1,
    )

    # Check the result
    assert result.node_count == 4
    assert result.relationship_count == 4
    assert "KiB" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
