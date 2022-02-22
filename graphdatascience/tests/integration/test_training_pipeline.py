from typing import Generator

import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.pipeline.lp_training_pipeline import LPTrainingPipeline
from graphdatascience.pipeline.nc_training_pipeline import NCTrainingPipeline
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner

PIPE_NAME = "pipe"


@pytest.fixture
def G(runner: Neo4jQueryRunner, gds: GraphDataScience) -> Generator[Graph, None, None]:
    runner.run_query(
        """
        CREATE
        (a: Node),
        (b: Node),
        (c: Node),
        (d: Node),
        (e: Node),
        (f: Node),
        (g: Node),
        (a)-[:REL]->(b),
        (a)-[:REL]->(c),
        (a)-[:REL]->(d),
        (a)-[:REL]->(e),
        (a)-[:REL]->(f),
        (a)-[:REL]->(g),
        (b)-[:REL]->(c),
        (b)-[:REL]->(a),
        (b)-[:REL]->(d),
        (b)-[:REL]->(e),
        (b)-[:REL]->(f),
        (b)-[:REL]->(g),
        (c)-[:REL]->(a),
        (c)-[:REL]->(b),
        (c)-[:REL]->(d),
        (c)-[:REL]->(e),
        (c)-[:REL]->(f),
        (c)-[:REL]->(g)
        """
    )
    G = gds.graph.project("g", "*", "*")

    yield G

    runner.run_query("MATCH (n) DETACH DELETE n")
    G.drop()


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
def nc_pipe(
    runner: Neo4jQueryRunner, gds: GraphDataScience
) -> Generator[NCTrainingPipeline, None, None]:
    pipe, _ = gds.alpha.ml.pipeline.nodeClassification.create(PIPE_NAME)

    yield pipe

    query = "CALL gds.beta.model.drop($name)"
    params = {"name": pipe.name()}
    runner.run_query(query, params)


def test_create_lp_pipeline(runner: Neo4jQueryRunner, gds: GraphDataScience) -> None:
    pipe, result = gds.alpha.ml.pipeline.linkPrediction.create("hello")
    assert pipe.name() == "hello"
    assert result["name"] == pipe.name()

    query = "CALL gds.beta.model.exists($name)"
    params = {"name": pipe.name()}
    result2 = runner.run_query(query, params)
    assert result2[0]["exists"]

    query = "CALL gds.beta.model.drop($name)"
    params = {"name": pipe.name()}
    runner.run_query(query, params)


def test_add_node_property_lp_pipeline(
    runner: Neo4jQueryRunner, lp_pipe: LPTrainingPipeline
) -> None:
    lp_pipe.addNodeProperty(
        "pageRank", mutateProperty="rank", dampingFactor=0.2, tolerance=0.3
    )

    query = "CALL gds.beta.model.list($name)"
    params = {"name": lp_pipe.name()}
    model_info = runner.run_query(query, params)[0]["modelInfo"]

    steps = model_info["featurePipeline"]["nodePropertySteps"]
    assert len(steps) == 1
    assert steps[0]["name"] == "gds.pageRank.mutate"


def test_add_feature_lp_pipeline(
    runner: Neo4jQueryRunner, lp_pipe: LPTrainingPipeline
) -> None:
    lp_pipe.addNodeProperty("degree", mutateProperty="rank")

    lp_pipe.addFeature("l2", nodeProperties=["degree"])

    query = "CALL gds.beta.model.list($name)"
    params = {"name": lp_pipe.name()}
    model_info = runner.run_query(query, params)[0]["modelInfo"]

    steps = model_info["featurePipeline"]["featureSteps"]
    assert len(steps) == 1
    assert steps[0]["name"] == "L2"


def test_configure_split_lp_pipeline(
    runner: Neo4jQueryRunner, lp_pipe: LPTrainingPipeline
) -> None:
    lp_pipe.configureSplit(trainFraction=0.42)

    query = "CALL gds.beta.model.list($name)"
    params = {"name": lp_pipe.name()}
    model_info = runner.run_query(query, params)[0]["modelInfo"]

    assert model_info["splitConfig"]["trainFraction"] == 0.42


def test_configure_params_lp_pipeline(
    runner: Neo4jQueryRunner, lp_pipe: LPTrainingPipeline
) -> None:
    lp_pipe.configureParams([{"tolerance": 0.01}, {"maxEpochs": 500}])

    query = "CALL gds.beta.model.list($name)"
    params = {"name": lp_pipe.name()}
    model_info = runner.run_query(query, params)[0]["modelInfo"]

    parameter_space = model_info["trainingParameterSpace"]
    assert len(parameter_space) == 2
    assert parameter_space[0]["tolerance"] == 0.01
    assert parameter_space[1]["maxEpochs"] == 500


def test_train_lp_pipeline(
    runner: Neo4jQueryRunner, lp_pipe: LPTrainingPipeline, G: Graph
) -> None:
    lp_pipe.addNodeProperty("degree", mutateProperty="rank")
    lp_pipe.addFeature("l2", nodeProperties=["rank"])
    lp_pipe.configureSplit(trainFraction=0.2, testFraction=0.2)

    lp_trained_pipe, result = lp_pipe.train(G, modelName="m", concurrency=2)
    assert lp_trained_pipe.name() == "m"
    assert result["configuration"]["modelName"] == "m"

    query = "CALL gds.beta.model.drop($name)"
    params = {"name": "m"}
    runner.run_query(query, params)


def test_node_property_steps_lp_pipeline(lp_pipe: LPTrainingPipeline) -> None:
    assert len(lp_pipe.node_property_steps()) == 0

    lp_pipe.addNodeProperty(
        "pageRank", mutateProperty="rank", dampingFactor=0.2, tolerance=0.3
    )

    steps = lp_pipe.node_property_steps()
    assert len(steps) == 1
    assert steps[0]["name"] == "gds.pageRank.mutate"


def test_feature_steps_lp_pipeline(lp_pipe: LPTrainingPipeline) -> None:
    lp_pipe.addNodeProperty("degree", mutateProperty="rank")
    assert len(lp_pipe.feature_steps()) == 0

    lp_pipe.addFeature("l2", nodeProperties=["degree"])

    steps = lp_pipe.feature_steps()
    assert len(steps) == 1
    assert steps[0]["name"] == "L2"


def test_split_config_lp_pipeline(lp_pipe: LPTrainingPipeline) -> None:
    split_config = lp_pipe.split_config()
    assert "trainFraction" in split_config.keys()


def test_parameter_space_lp_pipeline(lp_pipe: LPTrainingPipeline) -> None:
    parameter_space = lp_pipe.parameter_space()
    assert len(parameter_space) > 0
    assert "penalty" in parameter_space[0]


def test_select_features_nc_pipeline(
    runner: Neo4jQueryRunner, nc_pipe: NCTrainingPipeline
) -> None:
    nc_pipe.addNodeProperty("degree", mutateProperty="rank")

    nc_pipe.selectFeatures("rank")

    query = "CALL gds.beta.model.list($name)"
    params = {"name": nc_pipe.name()}
    model_info = runner.run_query(query, params)[0]["modelInfo"]

    steps = model_info["featurePipeline"]["featureProperties"]
    assert len(steps) == 1
    assert steps[0]["feature"] == "rank"


def test_feature_properties_nc_pipeline(nc_pipe: NCTrainingPipeline) -> None:
    nc_pipe.addNodeProperty("degree", mutateProperty="rank")
    assert len(nc_pipe.feature_properties()) == 0

    nc_pipe.selectFeatures("rank")

    steps = nc_pipe.feature_properties()
    assert len(steps) == 1
    assert steps[0]["feature"] == "rank"
