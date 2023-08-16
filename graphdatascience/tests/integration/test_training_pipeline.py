from typing import Generator

import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.pipeline.lp_training_pipeline import LPTrainingPipeline
from graphdatascience.pipeline.nc_training_pipeline import NCTrainingPipeline
from graphdatascience.pipeline.nr_training_pipeline import NRTrainingPipeline
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.server_version.server_version import ServerVersion

PIPE_NAME = "pipe"


@pytest.fixture
def G(runner: Neo4jQueryRunner, gds: GraphDataScience) -> Generator[Graph, None, None]:
    runner.run_query(
        """
        CREATE
        (a: Node {age: 12, fraudster: 0}),
        (b: Node {age:  1, fraudster: 1}),
        (c: Node {age:  7, fraudster: 0}),
        (d: Node {age: 54, fraudster: 1}),
        (e: Node {age: 18, fraudster: 0}),
        (f: Node {age: 23, fraudster: 1}),
        (g: Node {age: 49, fraudster: 0}),
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
    G, _ = gds.graph.project(
        "g", {"Node": {"properties": ["age", "fraudster"]}}, {"REL": {"orientation": "UNDIRECTED"}}
    )

    yield G

    runner.run_query("MATCH (n) DETACH DELETE n")
    G.drop()


@pytest.fixture
def lp_pipe(runner: Neo4jQueryRunner, gds: GraphDataScience) -> Generator[LPTrainingPipeline, None, None]:
    pipe, _ = gds.beta.pipeline.linkPrediction.create(PIPE_NAME)

    yield pipe

    pipe.drop()


@pytest.fixture
def nc_pipe(runner: Neo4jQueryRunner, gds: GraphDataScience) -> Generator[NCTrainingPipeline, None, None]:
    pipe, _ = gds.beta.pipeline.nodeClassification.create(PIPE_NAME)

    yield pipe

    pipe.drop()


@pytest.fixture
def nr_pipe(runner: Neo4jQueryRunner, gds: GraphDataScience) -> Generator[NRTrainingPipeline, None, None]:
    pipe, _ = gds.alpha.pipeline.nodeRegression.create(PIPE_NAME)

    yield pipe

    pipe.drop()


def test_create_lp_pipeline(runner: Neo4jQueryRunner, gds: GraphDataScience) -> None:
    pipe, result = gds.beta.pipeline.linkPrediction.create("hello")
    assert pipe.name() == "hello"
    assert result["name"] == pipe.name()

    query = "CALL gds.pipeline.exists($name)"
    params = {"name": pipe.name()}
    result2 = runner.run_query(query, params)
    assert result2["exists"][0]

    pipe.drop()


def test_add_node_property_lp_pipeline(runner: Neo4jQueryRunner, lp_pipe: LPTrainingPipeline) -> None:
    result = lp_pipe.addNodeProperty("pageRank", mutateProperty="rank", dampingFactor=0.2, tolerance=0.3)
    assert len(result["nodePropertySteps"]) == 1

    steps = lp_pipe.node_property_steps()
    assert len(steps) == 1
    assert steps["name"][0] == "gds.pageRank.mutate"


def test_add_feature_lp_pipeline(runner: Neo4jQueryRunner, lp_pipe: LPTrainingPipeline) -> None:
    lp_pipe.addNodeProperty("degree", mutateProperty="rank")

    result = lp_pipe.addFeature("l2", nodeProperties=["degree"])
    assert result["featureSteps"][0]["name"] == "L2"

    steps = lp_pipe.feature_steps()
    assert len(steps) == 1
    assert steps["name"][0] == "L2"


def test_configure_split_lp_pipeline(runner: Neo4jQueryRunner, lp_pipe: LPTrainingPipeline) -> None:
    result = lp_pipe.configureSplit(trainFraction=0.42)
    assert result["splitConfig"]["trainFraction"] == 0.42

    assert lp_pipe.split_config()["trainFraction"] == 0.42


@pytest.mark.compatible_with(max_exclusive=ServerVersion(2, 2, 0))
def test_train_unfiltered_lp_pipeline(runner: Neo4jQueryRunner, lp_pipe: LPTrainingPipeline, G: Graph) -> None:
    lp_pipe.addNodeProperty("degree", mutateProperty="rank")
    lp_pipe.addFeature("l2", nodeProperties=["rank"])
    lp_pipe.configureSplit(trainFraction=0.2, testFraction=0.2, validationFolds=2)
    lp_pipe.addLogisticRegression(penalty=1)

    lp_model, result = lp_pipe.train(G, modelName="m", concurrency=2)
    assert lp_model.name() == "m"
    assert result["configuration"]["modelName"] == "m"

    lp_model.drop()


@pytest.mark.compatible_with(max_exclusive=ServerVersion(2, 2, 0))
def test_train_estimate_unfiltered_lp_pipeline(lp_pipe: LPTrainingPipeline, G: Graph) -> None:
    lp_pipe.addLogisticRegression()
    result = lp_pipe.train_estimate(G, modelName="m", concurrency=2)
    assert result["requiredMemory"]


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_train_lp_pipeline(runner: Neo4jQueryRunner, lp_pipe: LPTrainingPipeline, G: Graph) -> None:
    lp_pipe.addNodeProperty("degree", mutateProperty="rank")
    lp_pipe.addFeature("l2", nodeProperties=["rank"])
    lp_pipe.configureSplit(trainFraction=0.2, testFraction=0.2, validationFolds=2)
    lp_pipe.addLogisticRegression(penalty=1)

    lp_model, result = lp_pipe.train(
        G,
        modelName="m",
        concurrency=2,
        sourceNodeLabel="Node",
        targetNodeLabel="Node",
        targetRelationshipType="REL",
    )
    assert lp_model.name() == "m"
    assert result["configuration"]["modelName"] == "m"

    lp_model.drop()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_filtered_train_estimate_lp_pipeline(lp_pipe: LPTrainingPipeline, G: Graph) -> None:
    lp_pipe.addLogisticRegression()
    result = lp_pipe.train_estimate(
        G,
        modelName="m",
        concurrency=2,
        sourceNodeLabel="Node",
        targetNodeLabel="Node",
        targetRelationshipType="REL",
    )
    assert result["requiredMemory"]


def test_node_property_steps_lp_pipeline(lp_pipe: LPTrainingPipeline) -> None:
    assert len(lp_pipe.node_property_steps()) == 0

    lp_pipe.addNodeProperty("pageRank", mutateProperty="rank", dampingFactor=0.2, tolerance=0.3)

    steps = lp_pipe.node_property_steps()
    assert len(steps) == 1
    assert steps["name"][0] == "gds.pageRank.mutate"


def test_feature_steps_lp_pipeline(lp_pipe: LPTrainingPipeline) -> None:
    lp_pipe.addNodeProperty("degree", mutateProperty="rank")
    assert len(lp_pipe.feature_steps()) == 0

    lp_pipe.addFeature("l2", nodeProperties=["degree"])

    steps = lp_pipe.feature_steps()
    assert len(steps) == 1
    assert steps["name"][0] == "L2"


def test_split_config_lp_pipeline(lp_pipe: LPTrainingPipeline) -> None:
    split_config = lp_pipe.split_config()
    assert "trainFraction" in split_config.keys()


def test_add_logistic_regression_lp_pipeline(lp_pipe: LPTrainingPipeline) -> None:
    res = lp_pipe.addLogisticRegression(penalty=42)
    lr_parameter_space = res["parameterSpace"]["LogisticRegression"]
    assert lr_parameter_space[0]["penalty"] == 42


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_add_logistic_regression_with_range_lp_pipeline(lp_pipe: LPTrainingPipeline) -> None:
    res = lp_pipe.addLogisticRegression(penalty=(42, 1337))
    lr_parameter_space = res["parameterSpace"]["LogisticRegression"]
    assert lr_parameter_space[0]["penalty"] == {"range": [42, 1337]}


def test_add_logistic_regression_with_bad_range_lp_pipeline(lp_pipe: LPTrainingPipeline) -> None:
    with pytest.raises(Exception):
        lp_pipe.addLogisticRegression(penalty=(42, 1, 1337))


def test_add_random_forest_lp_pipeline(lp_pipe: LPTrainingPipeline) -> None:
    res = lp_pipe.addRandomForest(maxDepth=1337)
    rf_parameter_space = res["parameterSpace"]["RandomForest"]
    assert rf_parameter_space[0]["maxDepth"] == 1337


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_add_mlp_with_wrong_config_lp_pipeline(lp_pipe: LPTrainingPipeline) -> None:
    with pytest.raises(Exception):
        lp_pipe.addMLP(hiddenLayerSizes=64)


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_add_mlp_lp_pipeline(lp_pipe: LPTrainingPipeline) -> None:
    res = lp_pipe.addMLP(hiddenLayerSizes=[64, 16, 4], penalty=0.1)
    mlp_parameter_space = res["parameterSpace"]["MultilayerPerceptron"]
    assert mlp_parameter_space[0]["hiddenLayerSizes"] == [64, 16, 4]
    assert mlp_parameter_space[0]["penalty"] == 0.1


def test_parameter_space_lp_pipeline(lp_pipe: LPTrainingPipeline) -> None:
    lp_pipe.addLogisticRegression()
    parameter_space = lp_pipe.parameter_space()
    assert len(parameter_space.keys()) >= 2
    assert "penalty" in parameter_space["LogisticRegression"][0]


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_auto_tuning_config_lp_pipeline(lp_pipe: LPTrainingPipeline) -> None:
    tuning_config = lp_pipe.auto_tuning_config()
    assert "maxTrials" in tuning_config


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_configure_auto_tuning_lp_pipeline(lp_pipe: LPTrainingPipeline) -> None:
    maxTrials = 1337
    result = lp_pipe.configureAutoTuning(maxTrials=maxTrials)
    assert result["autoTuningConfig"]["maxTrials"] == maxTrials


def test_select_features_nc_pipeline(runner: Neo4jQueryRunner, nc_pipe: NCTrainingPipeline) -> None:
    nc_pipe.addNodeProperty("degree", mutateProperty="rank")

    result = nc_pipe.selectFeatures("rank")
    assert result["featureProperties"][0] == "rank"

    steps = nc_pipe.feature_properties()
    assert len(steps) == 1
    assert steps[0]["feature"] == "rank"


def test_feature_properties_nc_pipeline(nc_pipe: NCTrainingPipeline) -> None:
    nc_pipe.addNodeProperty("degree", mutateProperty="rank")
    assert len(nc_pipe.feature_properties()) == 0

    nc_pipe.selectFeatures("rank")

    steps = nc_pipe.feature_properties()
    assert len(steps) == 1
    assert steps[0]["feature"] == "rank"


def test_train_nc_pipeline(runner: Neo4jQueryRunner, nc_pipe: NCTrainingPipeline, G: Graph) -> None:
    nc_pipe.addNodeProperty("degree", mutateProperty="rank")
    nc_pipe.selectFeatures(["rank"])
    nc_pipe.configureSplit(testFraction=0.2, validationFolds=2)
    nc_pipe.addLogisticRegression(penalty=1)

    nc_model, result = nc_pipe.train(G, modelName="m", concurrency=2, targetProperty="fraudster", metrics=["ACCURACY"])
    assert nc_model.name() == "m"
    assert result["configuration"]["modelName"] == "m"

    nc_model.drop()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_train_nr_pipeline(runner: Neo4jQueryRunner, nr_pipe: NRTrainingPipeline, G: Graph) -> None:
    nr_pipe.addNodeProperty("degree", mutateProperty="rank")
    nr_pipe.selectFeatures(["rank"])
    nr_pipe.configureSplit(testFraction=0.2, validationFolds=2)
    nr_pipe.addLinearRegression(penalty=1)

    nr_model, result = nr_pipe.train(
        G, modelName="m", concurrency=2, targetProperty="age", metrics=["MEAN_SQUARED_ERROR"]
    )
    assert nr_model.name() == "m"
    assert result["configuration"]["modelName"] == "m"

    nr_model.drop()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_add_random_forest_nr_pipeline(nr_pipe: NRTrainingPipeline) -> None:
    res = nr_pipe.addRandomForest(maxDepth=1337)
    rf_parameter_space = res["parameterSpace"]["RandomForest"]
    assert rf_parameter_space[0]["maxDepth"] == 1337


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_add_linear_regression_nr_pipeline(nr_pipe: NRTrainingPipeline) -> None:
    res = nr_pipe.addLinearRegression(penalty=42)
    nr_parameter_space = res["parameterSpace"]["LinearRegression"]
    assert nr_parameter_space[0]["penalty"] == 42
