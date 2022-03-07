from typing import Generator

import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.model.graphsage_model import GraphSageModel
from graphdatascience.model.link_prediction_model import LPModel
from graphdatascience.model.model import Model
from graphdatascience.model.node_classification_model import NCModel
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner

PIPE_NAME = "pipe"


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
    G, _ = gds.graph.project(
        "g", {"Node": {"properties": ["age"]}}, {"REL": {"orientation": "UNDIRECTED"}}
    )

    yield G

    runner.run_query("MATCH (n) DETACH DELETE n")
    G.drop()


@pytest.fixture(scope="module")
def lp_model(
    runner: Neo4jQueryRunner, gds: GraphDataScience, G: Graph
) -> Generator[Model, None, None]:
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

    lp_model.drop()


@pytest.fixture(scope="module")
def nc_model(
    runner: Neo4jQueryRunner, gds: GraphDataScience, G: Graph
) -> Generator[Model, None, None]:
    pipe, _ = gds.beta.pipeline.nodeClassification.create("pipe")

    try:
        pipe.addNodeProperty("degree", mutateProperty="rank")
        pipe.selectFeatures("rank")
        pipe.configureSplit(testFraction=0.3)
        nc_model, _ = pipe.train(
            G, modelName="nc-model", targetProperty="age", metrics=["ACCURACY"]
        )
    finally:
        query = "CALL gds.beta.pipeline.drop($name)"
        params = {"name": "pipe"}
        runner.run_query(query, params)

    yield nc_model

    nc_model.drop()


@pytest.fixture
def gs_model(
    runner: Neo4jQueryRunner, gds: GraphDataScience, G: Graph
) -> Generator[GraphSageModel, None, None]:
    model, _ = gds.beta.graphSage.train(
        G, modelName="gs-model", featureProperties=["age"]
    )

    yield model

    query = "CALL gds.beta.model.drop($name, false)"
    params = {"name": model.name()}
    runner.run_query(query, params)


def test_model_list(gds: GraphDataScience, lp_model: LPModel) -> None:
    result = gds.beta.model.list()

    assert len(result) == 1
    assert result[0]["modelInfo"]["modelName"] == lp_model.name()


def test_model_exists(gds: GraphDataScience) -> None:
    assert not gds.beta.model.exists("NOTHING")["exists"]


@pytest.mark.enterprise
def test_model_publish(
    runner: Neo4jQueryRunner, gds: GraphDataScience, gs_model: GraphSageModel
) -> None:
    assert not gs_model.shared()

    shared_model = gds.alpha.model.publish(gs_model)

    assert shared_model.shared()
    assert isinstance(shared_model, GraphSageModel)

    query = "CALL gds.beta.model.drop($name)"
    params = {"name": shared_model.name()}
    runner.run_query(query, params)


@pytest.mark.model_store_location
def test_model_load(
    runner: Neo4jQueryRunner, gds: GraphDataScience, gs_model: GraphSageModel
) -> None:
    runner.run_query(f"CALL gds.alpha.model.store('{gs_model.name()}')")
    runner.run_query(f"CALL gds.beta.model.drop('{gs_model.name()}')")

    model, result = gds.alpha.model.load(gs_model.name())
    assert isinstance(model, GraphSageModel)
    assert result["loadMillis"] >= 0

    runner.run_query(f"CALL gds.beta.model.drop('{gs_model.name()}')")
    runner.run_query(f"CALL gds.alpha.model.delete('{model.name()}')")


@pytest.mark.model_store_location
def test_model_store(
    runner: Neo4jQueryRunner, gds: GraphDataScience, gs_model: GraphSageModel
) -> None:
    model_name = gds.alpha.model.store(gs_model)["modelName"]

    # Should be deletable now
    runner.run_query(f"CALL gds.beta.model.drop('{gs_model.name()}')")
    runner.run_query(f"CALL gds.alpha.model.delete('{model_name}')")


@pytest.mark.model_store_location
def test_model_delete(
    runner: Neo4jQueryRunner, gds: GraphDataScience, gs_model: GraphSageModel
) -> None:
    model_name = runner.run_query(f"CALL gds.alpha.model.store('{gs_model.name()}')")[
        0
    ]["modelName"]

    model = gds.model.get(model_name)
    runner.run_query(f"CALL gds.beta.model.drop('{gs_model.name()}')")
    assert gds.alpha.model.delete(model)["deleteMillis"] >= 0

    res = runner.run_query(f"CALL gds.beta.model.exists('{model_name}')")
    assert not res[0]["exists"]


def test_model_drop(gds: GraphDataScience, G: Graph) -> None:
    model, _ = gds.beta.graphSage.train(G, modelName="hello", featureProperties=["age"])

    assert gds.beta.model.exists(model.name())["exists"]

    assert gds.beta.model.drop(model)["modelInfo"]["modelName"] == model.name()
    assert not gds.beta.model.exists(model.name())["exists"]


def test_model_get_lp_trained(gds: GraphDataScience, lp_model: LPModel) -> None:
    new_model = gds.model.get(lp_model.name())

    assert new_model.type() == lp_model.type()
    assert new_model.name() == lp_model.name()
    assert isinstance(new_model, LPModel)


def test_model_get_nc_trained(gds: GraphDataScience, nc_model: NCModel) -> None:
    new_model = gds.model.get(nc_model.name())

    assert new_model.type() == nc_model.type()
    assert new_model.name() == nc_model.name()
    assert isinstance(new_model, NCModel)


def test_model_get_graphsage(gds: GraphDataScience, gs_model: GraphSageModel) -> None:
    model = gds.model.get(gs_model.name())

    assert model.type() == gs_model.type()
    assert model.name() == gs_model.name()
    assert isinstance(model, GraphSageModel)

    model.drop()
