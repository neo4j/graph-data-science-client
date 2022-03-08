from typing import Generator

import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.model.link_prediction_model import LPModel
from graphdatascience.model.model import Model
from graphdatascience.model.node_classification_model import NCModel
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


@pytest.fixture(scope="module")
def G(runner: Neo4jQueryRunner, gds: GraphDataScience) -> Generator[Graph, None, None]:
    runner.run_query(
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

    runner.run_query("MATCH (n) DETACH DELETE n")
    G.drop()


@pytest.fixture(scope="module")
def lp_model(runner: Neo4jQueryRunner, gds: GraphDataScience, G: Graph) -> Generator[Model, None, None]:
    pipe, _ = gds.beta.pipeline.linkPrediction.create("pipe")

    try:
        pipe.addNodeProperty("degree", mutateProperty="rank")
        pipe.addFeature("l2", nodeProperties=["rank"])
        pipe.configureSplit(trainFraction=0.4, testFraction=0.2)
        lp_model, _ = pipe.train(G, modelName="lp-model", concurrency=2)
    finally:
        query = "CALL gds.beta.pipeline.drop($name)"
        params = {"name": "pipe"}
        runner.run_query(query, params)

    yield lp_model

    query = "CALL gds.beta.model.drop($name)"
    params = {"name": lp_model.name()}
    runner.run_query(query, params)


@pytest.fixture(scope="module")
def nc_model(runner: Neo4jQueryRunner, gds: GraphDataScience, G: Graph) -> Generator[Model, None, None]:
    pipe, _ = gds.beta.pipeline.nodeClassification.create("pipe")

    try:
        pipe.addNodeProperty("degree", mutateProperty="rank")
        pipe.selectFeatures("rank")
        pipe.configureSplit(testFraction=0.3)
        nc_model, _ = pipe.train(G, modelName="nc-model", targetProperty="age", metrics=["ACCURACY"])
    finally:
        query = "CALL gds.beta.pipeline.drop($name)"
        params = {"name": "pipe"}
        runner.run_query(query, params)

    yield nc_model

    query = "CALL gds.beta.model.drop($name)"
    params = {"name": nc_model.name()}
    runner.run_query(query, params)


def test_predict_stream_lp_model(lp_model: LPModel, G: Graph) -> None:
    result = lp_model.predict_stream(G, topN=2)
    assert len(result) == 2


def test_predict_mutate_lp_model(lp_model: LPModel, G: Graph) -> None:
    result = lp_model.predict_mutate(G, topN=2, mutateRelationshipType="PRED_REL")
    assert result["relationshipsWritten"] == 4


def test_estimate_predict_stream_nc_model(nc_model: NCModel, G: Graph) -> None:
    result = nc_model.predict_stream_estimate(G)
    assert result["requiredMemory"]


def test_predict_stream_nc_model(nc_model: NCModel, G: Graph) -> None:
    result = nc_model.predict_stream(G)
    assert len(result) == G.node_count()


def test_predict_mutate_nc_model(nc_model: NCModel, G: Graph) -> None:
    result = nc_model.predict_mutate(G, mutateProperty="whoa")
    assert result["nodePropertiesWritten"] == G.node_count()


def test_predict_write_nc_model(nc_model: NCModel, G: Graph) -> None:
    result = nc_model.predict_write(G, writeProperty="whoa")
    assert result["nodePropertiesWritten"] == G.node_count()


def test_type_nc_model(nc_model: NCModel) -> None:
    assert nc_model.type() == "NodeClassification"


def test_train_config_nc_model(nc_model: NCModel, G: Graph) -> None:
    train_config = nc_model.train_config()
    assert train_config["modelName"] == nc_model.name()
    assert train_config["graphName"] == G.name()


def test_graph_schema_nc_model(
    nc_model: NCModel,
) -> None:
    graph_schema = nc_model.graph_schema()
    assert "nodes" in graph_schema.keys()


def test_loaded_nc_model(nc_model: NCModel) -> None:
    assert nc_model.loaded()


def test_stored_nc_model(nc_model: NCModel) -> None:
    assert not nc_model.stored()


def test_creation_time_nc_model(
    nc_model: NCModel,
) -> None:
    assert nc_model.creation_time()


def test_shared_nc_model(nc_model: NCModel) -> None:
    assert not nc_model.shared()


def test_metrics_nc_model(nc_model: NCModel) -> None:
    assert "ACCURACY" in nc_model.metrics().keys()
