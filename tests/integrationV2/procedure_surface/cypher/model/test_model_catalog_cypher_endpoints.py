import datetime
from typing import Generator

import pytest

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.procedure_surface.api.model.model_catalog_endpoints import ModelCatalogEndpoints
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


@pytest.fixture
def gds(query_runner: Neo4jQueryRunner) -> GraphDataScience:
    return GraphDataScience(query_runner._driver, arrow=False)


@pytest.fixture
def sample_graph(gds: GraphDataScience) -> Generator[str, None, None]:
    gds.run_cypher(
        """
        CREATE
        (a: Node {age: 1}),
        (b: Node {age: 2}),
        (c: Node {age: 3}),
        (a)-[:REL]->(b),
        (b)-[:REL]->(c),
        (c)-[:REL]->(a)
        """
    )
    G, _ = gds.graph.project("model_catalog_cypher_g", {"Node": {"properties": ["age"]}}, "REL")

    yield G.name()

    G.drop()
    gds.run_cypher("MATCH (n) DETACH DELETE n")


@pytest.fixture
def gs_model_name(gds: GraphDataScience, sample_graph: str) -> Generator[str, None, None]:
    model_name = "gs-model-catalog-cypher"
    gds.run_cypher(
        f"CALL gds.beta.graphSage.train('{sample_graph}', {{modelName: '{model_name}', featureProperties: ['age']}})"
    )

    yield model_name

    gds.run_cypher(f"CALL gds.model.drop('{model_name}', false)")


@pytest.fixture
def model_catalog(gds: GraphDataScience) -> ModelCatalogEndpoints:
    return gds.v2.model


def test_model_list(gs_model_name: str, model_catalog: ModelCatalogEndpoints) -> None:
    models = model_catalog.list()
    assert any(m.name == gs_model_name for m in models)


def test_model_get(gs_model_name: str, model_catalog: ModelCatalogEndpoints) -> None:
    model = model_catalog.get(gs_model_name)
    assert model is not None
    assert model.name == gs_model_name
    assert model.type == "graphSage"
    assert model.loaded
    assert model.creation_time.date() == datetime.date.today()
    assert not model.stored

    assert model_catalog.get("nonexistent-model") is None


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
    assert model_catalog.drop("nonexistent-model", fail_if_missing=False) is None

    with pytest.raises(Exception, match="Model with name `nonexistent-model` does not exist"):
        model_catalog.drop("nonexistent-model", fail_if_missing=True)


def test_store_model(gs_model_name: str, model_catalog: ModelCatalogEndpoints) -> None:
    model_catalog.store(gs_model_name)

    stored = model_catalog.get(gs_model_name)
    assert stored is not None
    assert stored.stored
    assert stored.loaded

    model_catalog.drop(gs_model_name)
    dropped = model_catalog.get(gs_model_name)
    assert dropped is not None
    assert dropped.stored
    assert not dropped.loaded

    model_catalog.delete(gs_model_name)


def test_load_model(gs_model_name: str, model_catalog: ModelCatalogEndpoints) -> None:
    model_catalog.store(gs_model_name)
    model_catalog.drop(gs_model_name)

    stored = model_catalog.get(gs_model_name)
    assert stored is not None
    assert stored.stored
    assert not stored.loaded

    model_catalog.load(gs_model_name)

    loaded = model_catalog.get(gs_model_name)
    assert loaded is not None
    assert loaded.stored
    assert loaded.loaded

    model_catalog.delete(gs_model_name)


def test_delete_model(gs_model_name: str, model_catalog: ModelCatalogEndpoints) -> None:
    model_catalog.store(gs_model_name)

    stored = model_catalog.get(gs_model_name)
    assert stored is not None
    assert stored.stored

    model_catalog.delete(gs_model_name)

    deleted = model_catalog.get(gs_model_name)
    assert deleted is not None
    assert not deleted.stored
    assert deleted.loaded
