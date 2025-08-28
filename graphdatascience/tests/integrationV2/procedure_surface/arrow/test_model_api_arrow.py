import json
from typing import Generator

import pytest
from pyarrow.flight import FlightServerError

from graphdatascience import Graph
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.graphsage_train_arrow_endpoints import GraphSageTrainArrowEndpoints
from graphdatascience.procedure_surface.arrow.model_api_arrow import ModelApiArrow
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    gdl = """
    (a: Node {age: 1})
    (b: Node {age: 2})
    (c: Node {age: 3})
    (d: Node {age: 4})
    (e: Node {age: 5})
    (f: Node {age: 6})
    (a)-[:REL]->(b)
    (b)-[:REL]->(c)
    (c)-[:REL]->(a)
    (d)-[:REL]->(e)
    (e)-[:REL]->(f)
    (f)-[:REL]->(d)
    """

    yield create_graph(arrow_client, "model_api_g", gdl)
    arrow_client.do_action("v2/graph.drop", json.dumps({"graphName": "model_api_g"}).encode("utf-8"))


@pytest.fixture
def gs_model(arrow_client: AuthenticatedArrowClient, sample_graph: Graph) -> Generator[str, None, None]:
    model, _ = GraphSageTrainArrowEndpoints(arrow_client).train(
        G=sample_graph,
        model_name="gs-model",
        feature_properties=["age"],
        embedding_dimension=1,
        sample_sizes=[1],
        max_iterations=1,
        search_depth=1,
    )

    yield model.name()

    arrow_client.do_action_with_retry("v2/model.drop", json.dumps({"modelName": model.name()}).encode("utf-8"))


@pytest.fixture
def model_api(arrow_client: AuthenticatedArrowClient) -> Generator[ModelApiArrow, None, None]:
    yield ModelApiArrow(arrow_client)


def test_model_get(gs_model: str, model_api: ModelApiArrow) -> None:
    model = model_api.get(gs_model)

    assert model.name == gs_model
    assert model.type == "graphSage"

    with pytest.raises(ValueError, match="There is no 'nonexistent-model' in the model catalog"):
        model_api.get("nonexistent-model")


def test_model_exists(gs_model: str, model_api: ModelApiArrow) -> None:
    assert model_api.exists(gs_model)
    assert not model_api.exists("nonexistent-model")


def test_model_delete(gs_model: str, model_api: ModelApiArrow) -> None:
    model_details = model_api.drop(gs_model, fail_if_missing=False)

    assert model_details is not None
    assert model_details.name == gs_model

    # Check that the model no longer exists
    assert not model_api.exists(gs_model)

    # Attempt to drop a non-existing model
    assert model_api.drop("nonexistent-model", fail_if_missing=False) is None

    with pytest.raises(FlightServerError, match="Model with name `nonexistent-model` does not exist"):
        model_api.drop("nonexistent-model", fail_if_missing=True)
