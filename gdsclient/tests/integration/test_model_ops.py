from typing import Generator

import pytest

from gdsclient.graph_data_science import GraphDataScience
from gdsclient.pipeline.lp_training_pipeline import LPTrainingPipeline
from gdsclient.query_runner.neo4j_query_runner import Neo4jQueryRunner

PIPE_NAME = "pipe"


@pytest.fixture
def lp_pipe(
    runner: Neo4jQueryRunner, gds: GraphDataScience
) -> Generator[LPTrainingPipeline, None, None]:
    pipe = gds.alpha.ml.pipeline.linkPrediction.create(PIPE_NAME)

    yield pipe

    query = "CALL gds.beta.model.drop($name)"
    params = {"name": pipe.name()}
    runner.run_query(query, params)


def test_model_list(gds: GraphDataScience, lp_pipe: LPTrainingPipeline) -> None:
    result = gds.beta.model.list()

    assert len(result) == 1
    assert result[0]["modelInfo"]["modelName"] == lp_pipe.name()


def test_model_exists(gds: GraphDataScience, lp_pipe: LPTrainingPipeline) -> None:
    assert not gds.beta.model.exists("NOTHING")[0]["exists"]
    assert gds.beta.model.exists(lp_pipe)[0]["exists"]


@pytest.mark.enterprise
def test_model_publish(runner: Neo4jQueryRunner, gds: GraphDataScience) -> None:
    pipe = gds.alpha.ml.pipeline.linkPrediction.create(PIPE_NAME)

    assert not pipe.shared()

    shared_pipe = gds.alpha.model.publish(pipe)

    assert shared_pipe.shared()

    query = "CALL gds.beta.model.drop($name)"
    params = {"name": shared_pipe.name()}
    runner.run_query(query, params)


def test_model_drop(gds: GraphDataScience) -> None:
    pipe = gds.alpha.ml.pipeline.linkPrediction.create(PIPE_NAME)
    assert gds.beta.model.exists(pipe)[0]["exists"]

    assert gds.beta.model.drop(pipe)[0]["modelInfo"]["modelName"] == pipe.name()
    assert not gds.beta.model.exists(pipe)[0]["exists"]
