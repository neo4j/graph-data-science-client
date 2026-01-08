import datetime
import json
from typing import Generator

import pytest
from pyarrow.flight import FlightServerError

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.model.model_catalog_endpoints import ModelCatalogEndpoints
from graphdatascience.procedure_surface.arrow.model.model_catalog_arrow_endpoints import ModelCatalogArrowEndpoints
from graphdatascience.procedure_surface.arrow.node_embedding.graphsage_train_arrow_endpoints import (
    GraphSageTrainArrowEndpoints,
)
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
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

    with create_graph(arrow_client, "model_catalog_api_g", gdl) as G:
        yield G


@pytest.fixture
def gs_model_name(arrow_client: AuthenticatedArrowClient, sample_graph: GraphV2) -> Generator[str, None, None]:
    model_name = "gs-model-catalog"
    model, _ = GraphSageTrainArrowEndpoints(arrow_client, None)(
        G=sample_graph,
        model_name=model_name,
        feature_properties=["age"],
        embedding_dimension=1,
        sample_sizes=[1],
        max_iterations=1,
        search_depth=1,
    )

    yield model.name()

    arrow_client.do_action_with_retry("v2/model.drop", json.dumps({"modelName": model_name}).encode("utf-8"))


@pytest.fixture
def model_catalog(arrow_client: AuthenticatedArrowClient) -> ModelCatalogEndpoints:
    return ModelCatalogArrowEndpoints(arrow_client)


def test_model_list(gs_model_name: str, model_catalog: ModelCatalogEndpoints) -> None:
    models = model_catalog.list()
    assert len(models) == 1
    model = models[0]

    assert model_catalog.get(model.name) == model


def test_model_get(gs_model_name: str, model_catalog: ModelCatalogEndpoints) -> None:
    model = model_catalog.get(gs_model_name)
    assert model is not None
    assert model.name == gs_model_name
    assert model.type == "graphSage"
    assert model.loaded
    assert model.creation_time.date() == datetime.date.today()
    assert not model.stored

    with pytest.raises(ValueError, match="Model with name `nonexistent-model` does not exist"):
        model_catalog.get("nonexistent-model")


def test_model_exists(gs_model_name: str, model_catalog: ModelCatalogEndpoints) -> None:
    result = model_catalog.exists(gs_model_name)
    assert result is not None
    assert result.model_name == gs_model_name
    assert result.exists is True

    assert model_catalog.exists("nonexistent-model") is None


def test_model_drop(gs_model_name: str, model_catalog: ModelCatalogEndpoints) -> None:
    model_details = model_catalog.drop(gs_model_name, fail_if_missing=False)

    assert model_details is not None
    assert model_details.name == gs_model_name

    # Check that the model no longer exists
    assert model_catalog.exists(gs_model_name) is None

    # Attempt to drop a non-existing model
    with pytest.raises(ValueError, match="Model with name `nonexistent-model` does not exist"):
        model_catalog.drop("nonexistent-model", fail_if_missing=False)

    with pytest.raises(FlightServerError, match="Model with name `nonexistent-model` does not exist"):
        model_catalog.drop("nonexistent-model", fail_if_missing=True)


def test_store_model(gs_model_name: str, model_catalog: ModelCatalogEndpoints) -> None:
    model_catalog.store(gs_model_name)

    stored = model_catalog.get(gs_model_name)
    assert stored.stored
    assert stored.loaded

    model_catalog.drop(gs_model_name)
    dropped = model_catalog.get(gs_model_name)
    assert dropped.stored
    assert not dropped.loaded

    model_catalog.delete(gs_model_name)

    with pytest.raises(ValueError, match="Model with name `nonexistent-model` does not exist"):
        model_catalog.store("nonexistent-model")


def test_load_model(gs_model_name: str, model_catalog: ModelCatalogEndpoints) -> None:
    model_catalog.store(gs_model_name)
    model_catalog.drop(gs_model_name)

    stored = model_catalog.get(gs_model_name)
    assert stored.stored
    assert not stored.loaded

    model_catalog.load(gs_model_name)

    loaded = model_catalog.get(gs_model_name)
    assert loaded.stored
    assert loaded.loaded

    model_catalog.delete(gs_model_name)

    with pytest.raises(ValueError, match="Model with name `nonexistent-model` does not exist"):
        model_catalog.load("nonexistent-model")


def test_delete_model(gs_model_name: str, model_catalog: ModelCatalogEndpoints) -> None:
    model_catalog.store(gs_model_name)

    stored = model_catalog.get(gs_model_name)
    assert stored.stored

    model_catalog.delete(gs_model_name)

    deleted = model_catalog.get(gs_model_name)
    assert not deleted.stored
    assert deleted.loaded

    with pytest.raises(ValueError, match="Model with name `nonexistent-model` does not exist"):
        model_catalog.delete("nonexistent-model")
