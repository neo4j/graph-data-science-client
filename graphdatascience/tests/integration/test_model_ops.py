from typing import Generator

import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.model.graphsage_model import GraphSageModel
from graphdatascience.pipeline.lp_prediction_pipeline import LPPredictionPipeline
from graphdatascience.pipeline.lp_training_pipeline import LPTrainingPipeline
from graphdatascience.pipeline.nc_prediction_pipeline import NCPredictionPipeline
from graphdatascience.pipeline.nc_training_pipeline import NCTrainingPipeline
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner

PIPE_NAME = "pipe"


@pytest.fixture
def lp_pipe(
    runner: Neo4jQueryRunner, gds: GraphDataScience
) -> Generator[LPTrainingPipeline, None, None]:
    pipe, _ = gds.alpha.ml.pipeline.linkPrediction.create(PIPE_NAME)

    yield pipe

    query = "CALL gds.beta.model.drop($name)"
    params = {"name": pipe.name()}
    runner.run_query(query, params)


@pytest.fixture
def gs_model(gds: GraphDataScience, G: Graph) -> Generator[GraphSageModel, None, None]:
    model, _ = gds.beta.graphSage.train(G, modelName="m", featureProperties=["age"])

    yield model

    # cannot clean up with drop here because of delete bug
    # model.drop()


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
    G = gds.graph.project(
        "g", {"Node": {"properties": ["age"]}}, {"REL": {"orientation": "UNDIRECTED"}}
    )

    yield G

    runner.run_query("MATCH (n) DETACH DELETE n")
    G.drop()


def test_model_list(gds: GraphDataScience, lp_pipe: LPTrainingPipeline) -> None:
    result = gds.beta.model.list()

    assert len(result) == 1
    assert result[0]["modelInfo"]["modelName"] == lp_pipe.name()


def test_model_exists(gds: GraphDataScience) -> None:
    assert not gds.beta.model.exists("NOTHING")["exists"]


@pytest.mark.enterprise
def test_model_publish(runner: Neo4jQueryRunner, gds: GraphDataScience) -> None:
    pipe, _ = gds.alpha.ml.pipeline.linkPrediction.create(PIPE_NAME)

    assert not pipe.shared()

    shared_pipe = gds.alpha.model.publish(pipe)

    assert shared_pipe.shared()
    assert isinstance(shared_pipe, LPTrainingPipeline)

    query = "CALL gds.beta.model.drop($name)"
    params = {"name": shared_pipe.name()}
    runner.run_query(query, params)


@pytest.mark.model_store_location
def test_model_load(
    runner: Neo4jQueryRunner, gds: GraphDataScience, gs_model: GraphSageModel
) -> None:
    runner.run_query(f"CALL gds.alpha.model.store('{gs_model.name()}')")
    runner.run_query(f"CALL gds.beta.model.drop('{gs_model.name()}')")

    model = gds.alpha.model.load(gs_model.name())
    assert isinstance(model, GraphSageModel)

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


def test_model_drop(gds: GraphDataScience) -> None:
    pipe, _ = gds.alpha.ml.pipeline.linkPrediction.create(PIPE_NAME)
    assert gds.beta.model.exists(pipe.name())["exists"]

    assert gds.beta.model.drop(pipe)["modelInfo"]["modelName"] == pipe.name()
    assert not gds.beta.model.exists(pipe.name())["exists"]


def test_model_get_lp(gds: GraphDataScience, lp_pipe: LPTrainingPipeline) -> None:
    pipe = gds.model.get(lp_pipe.name())

    assert pipe.name() == lp_pipe.name()
    assert pipe.stored() == lp_pipe.stored()
    assert isinstance(pipe, LPTrainingPipeline)
    assert pipe.feature_steps() == lp_pipe.feature_steps()


def test_model_get_nc(gds: GraphDataScience) -> None:
    nc_pipe, _ = gds.alpha.ml.pipeline.nodeClassification.create(PIPE_NAME)
    pipe = gds.model.get(nc_pipe.name())

    assert pipe.name() == nc_pipe.name()
    assert pipe.stored() == nc_pipe.stored()
    assert isinstance(pipe, NCTrainingPipeline)
    assert pipe.feature_properties() == nc_pipe.feature_properties()

    nc_pipe.drop()


def test_model_get_lp_trained(gds: GraphDataScience, G: Graph) -> None:
    lp_pipe, _ = gds.alpha.ml.pipeline.linkPrediction.create(PIPE_NAME)
    lp_pipe.addNodeProperty("degree", mutateProperty="rank")
    lp_pipe.addFeature("l2", nodeProperties=["rank"])
    lp_pipe.configureSplit(trainFraction=0.4, testFraction=0.2)
    lp_trained_pipe = lp_pipe.train(G, modelName="m", concurrency=2)

    pipe = gds.model.get(lp_trained_pipe.name())

    assert pipe.type() == lp_trained_pipe.type()
    assert pipe.name() == lp_trained_pipe.name()
    assert isinstance(pipe, LPPredictionPipeline)

    lp_pipe.drop()
    lp_trained_pipe.drop()


def test_model_get_nc_trained(gds: GraphDataScience, G: Graph) -> None:
    nc_pipe, _ = gds.alpha.ml.pipeline.nodeClassification.create(PIPE_NAME)
    nc_pipe.addNodeProperty("degree", mutateProperty="rank")
    nc_pipe.selectFeatures("rank")
    nc_pipe.configureSplit(testFraction=0.3)
    nc_trained_pipe = nc_pipe.train(
        G, modelName="n", targetProperty="age", metrics=["ACCURACY"]
    )

    pipe = gds.model.get(nc_trained_pipe.name())

    assert pipe.type() == nc_trained_pipe.type()
    assert pipe.name() == nc_trained_pipe.name()
    assert isinstance(pipe, NCPredictionPipeline)

    nc_pipe.drop()
    nc_trained_pipe.drop()


def test_model_get_graphsage(gds: GraphDataScience, gs_model: GraphSageModel) -> None:
    model = gds.model.get(gs_model.name())

    assert model.type() == gs_model.type()
    assert model.name() == gs_model.name()
    assert isinstance(model, GraphSageModel)

    model.drop()
