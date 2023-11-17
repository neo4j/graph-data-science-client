from typing import Generator

import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.model.graphsage_model import GraphSageModel
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.server_version.server_version import ServerVersion

MODEL_NAME = "gs-model"


@pytest.fixture
def G(runner: Neo4jQueryRunner, gds: GraphDataScience) -> Generator[Graph, None, None]:
    runner.run_cypher(
        """
        CREATE
        (a: Node {age: 2}),
        (b: Node {age: 3}),
        (c: Node {age: 2}),
        (d: Node {age: 1}),
        (e: Node {age: 2}),
        (a)-[:REL]->(b),
        (a)-[:REL]->(c),
        (b)-[:REL]->(c),
        (b)-[:REL]->(a),
        (c)-[:REL]->(a),
        (c)-[:REL]->(b)
        """
    )
    G, _ = gds.graph.project("g", {"Node": {"properties": ["age"]}}, {"REL": {"orientation": "UNDIRECTED"}})

    yield G

    runner.run_cypher("MATCH (n) DETACH DELETE n")
    G.drop()


@pytest.fixture
def gs_model(gds: GraphDataScience, G: Graph, runner: Neo4jQueryRunner) -> Generator[GraphSageModel, None, None]:
    model, _ = gds.beta.graphSage.train(G, modelName=MODEL_NAME, featureProperties=["age"])

    yield model

    namespace = "beta." if gds.server_version() < ServerVersion(2, 5, 0) else ""
    query = f"CALL gds.{namespace}model.drop($name)"
    params = {"name": model.name()}
    runner.run_cypher(query, params)


def test_model_exists(gs_model: GraphSageModel) -> None:
    assert gs_model.exists()


def test_model_drop(gds: GraphDataScience, G: Graph) -> None:
    model, _ = gds.beta.graphSage.train(G, modelName="gs-model", featureProperties=["age"])

    model_type = model.type()
    model_published = model.shared()
    drop_result = model.drop()
    if gds.server_version() >= ServerVersion(2, 5, 0):
        assert drop_result["modelName"] == model.name()
        assert drop_result["modelType"] == model_type
        assert drop_result["published"] == model_published
    assert drop_result["modelInfo"]["modelName"] == model.name()

    assert not model.exists()

    # Should not raise error.
    model.drop(failIfMissing=False)

    with pytest.raises(Exception):
        model.drop(failIfMissing=True)


def test_model_name(gs_model: GraphSageModel) -> None:
    assert gs_model.name() == MODEL_NAME


def test_model_type(gs_model: GraphSageModel) -> None:
    assert gs_model.type() == "graphSage"


def test_model_train_config(gs_model: GraphSageModel) -> None:
    assert gs_model.train_config()["modelName"] == MODEL_NAME


def test_model_graph_schema(gs_model: GraphSageModel) -> None:
    assert "age" in gs_model.graph_schema()["nodes"]["Node"].keys()


def test_model_loaded(gs_model: GraphSageModel) -> None:
    assert gs_model.loaded()


def test_model_stored(gs_model: GraphSageModel) -> None:
    assert not gs_model.stored()


def test_model_creation_time(gs_model: GraphSageModel) -> None:
    assert gs_model.creation_time().year > 2000


def test_model_shared(gs_model: GraphSageModel) -> None:
    assert not gs_model.shared()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_model_published(gs_model: GraphSageModel) -> None:
    assert not gs_model.published()


def test_model_metrics(gs_model: GraphSageModel) -> None:
    assert isinstance(gs_model.metrics()["didConverge"], bool)


def test_model_str(gs_model: GraphSageModel) -> None:
    assert str(gs_model) == "GraphSageModel(name=gs-model, type=graphSage)"


def test_model_repr(gs_model: GraphSageModel) -> None:
    assert "'metrics'" in repr(gs_model)


def test_model_info(gs_model: GraphSageModel) -> None:
    assert gs_model.model_info()["modelName"] == "gs-model"
