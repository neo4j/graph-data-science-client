from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.graphsage_train_arrow_endpoints import GraphSageTrainArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


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
    model, result = graphsage_endpoints.train(
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
