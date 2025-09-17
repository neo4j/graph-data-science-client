import json
from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.model.graphsage_model import GraphSageModelV2
from graphdatascience.procedure_surface.arrow.graphsage_train_arrow_endpoints import GraphSageTrainArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    gdl = """
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
def gs_model(arrow_client: AuthenticatedArrowClient, sample_graph: GraphV2) -> Generator[GraphSageModelV2, None, None]:
    model, _ = GraphSageTrainArrowEndpoints(arrow_client).train(
        G=sample_graph,
        model_name="gs-model",
        feature_properties=["feature"],
        embedding_dimension=1,
        sample_sizes=[1],
        max_iterations=1,
        search_depth=1,
    )

    yield model

    arrow_client.do_action_with_retry("v2/model.drop", json.dumps({"modelName": model.name()}).encode("utf-8"))


def test_stream(gs_model: GraphSageModelV2, sample_graph: GraphV2) -> None:
    result = gs_model.predict_stream(sample_graph, concurrency=4)

    assert set(result.columns) == {"nodeId", "embedding"}
    assert len(result) == 4


def test_mutate(gs_model: GraphSageModelV2, sample_graph: GraphV2) -> None:
    result = gs_model.predict_mutate(sample_graph, concurrency=4, mutate_property="embedding")

    assert result.node_properties_written == 4


def test_write(gs_model: GraphSageModelV2, sample_graph: GraphV2) -> None:
    with pytest.raises(Exception, match="Write back client is not initialized"):
        gs_model.predict_write(sample_graph, write_property="embedding", concurrency=4, write_concurrency=2)


def test_estimate(gs_model: GraphSageModelV2, sample_graph: GraphV2) -> None:
    result = gs_model.predict_estimate(sample_graph, concurrency=4)

    assert result.node_count == 4
    assert result.relationship_count == 4
    assert "KiB" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
