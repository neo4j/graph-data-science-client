from typing import Generator

import pytest

from gdsclient.graph_data_science import GraphDataScience
from gdsclient.pipeline.lp_pipeline import LPPipeline
from gdsclient.query_runner.neo4j_query_runner import Neo4jQueryRunner

PIPE_NAME = "pipe"


@pytest.fixture
def pipe(
    runner: Neo4jQueryRunner, gds: GraphDataScience
) -> Generator[LPPipeline, None, None]:
    pipe = gds.alpha.ml.pipeline.linkPrediction.create(PIPE_NAME)

    yield pipe

    query = "CALL gds.beta.model.drop($name)"
    params = {"name": pipe.name()}
    runner.run_query(query, params)


def test_create_lp_pipeline(runner: Neo4jQueryRunner, gds: GraphDataScience) -> None:
    pipe = gds.alpha.ml.pipeline.linkPrediction.create("hello")
    assert pipe.name() == "hello"

    query = "CALL gds.beta.model.exists($name)"
    params = {"name": pipe.name()}
    result = runner.run_query(query, params)
    assert result[0]["exists"]

    query = "CALL gds.beta.model.drop($name)"
    params = {"name": pipe.name()}
    runner.run_query(query, params)


def test_add_node_property_lp_pipeline(
    runner: Neo4jQueryRunner, pipe: LPPipeline
) -> None:
    pipe.addNodeProperty(
        "pageRank", mutateProperty="rank", dampingFactor=0.2, tolerance=0.3
    )

    query = "CALL gds.beta.model.list($name)"
    params = {"name": pipe.name()}
    model_info = runner.run_query(query, params)[0]["modelInfo"]

    steps = model_info["featurePipeline"]["nodePropertySteps"]
    assert len(steps) == 1
    assert steps[0]["name"] == "gds.pageRank.mutate"


def test_add_feature_lp_pipeline(runner: Neo4jQueryRunner, pipe: LPPipeline) -> None:
    pipe.addNodeProperty("degree", mutateProperty="rank")

    pipe.addFeature("l2", nodeProperties=["degree"])

    query = "CALL gds.beta.model.list($name)"
    params = {"name": pipe.name()}
    model_info = runner.run_query(query, params)[0]["modelInfo"]

    steps = model_info["featurePipeline"]["featureSteps"]
    assert len(steps) == 1
    assert steps[0]["name"] == "L2"


def test_node_property_steps_lp_pipeline(
    runner: Neo4jQueryRunner, pipe: LPPipeline
) -> None:
    assert len(pipe.node_property_steps()) == 0

    pipe.addNodeProperty(
        "pageRank", mutateProperty="rank", dampingFactor=0.2, tolerance=0.3
    )

    steps = pipe.node_property_steps()
    assert len(steps) == 1
    assert steps[0]["name"] == "gds.pageRank.mutate"


def test_feature_steps_lp_pipeline(runner: Neo4jQueryRunner, pipe: LPPipeline) -> None:
    pipe.addNodeProperty("degree", mutateProperty="rank")
    assert len(pipe.feature_steps()) == 0

    pipe.addFeature("l2", nodeProperties=["degree"])

    steps = pipe.feature_steps()

    assert len(steps) == 1
    assert steps[0]["name"] == "L2"
