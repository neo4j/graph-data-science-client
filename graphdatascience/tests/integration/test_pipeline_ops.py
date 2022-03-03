from typing import Generator

import pytest

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.pipeline.lp_training_pipeline import LPTrainingPipeline
from graphdatascience.pipeline.nc_training_pipeline import NCTrainingPipeline
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner

PIPE_NAME = "pipe"


@pytest.fixture
def lp_pipe(
    runner: Neo4jQueryRunner, gds: GraphDataScience
) -> Generator[LPTrainingPipeline, None, None]:
    pipe, _ = gds.beta.pipeline.linkPrediction.create(PIPE_NAME)

    yield pipe

    query = "CALL gds.beta.pipeline.drop($name)"
    params = {"name": pipe.name()}
    runner.run_query(query, params)


@pytest.fixture
def nc_pipe(
    runner: Neo4jQueryRunner, gds: GraphDataScience
) -> Generator[NCTrainingPipeline, None, None]:
    pipe, _ = gds.beta.pipeline.nodeClassification.create(PIPE_NAME)

    yield pipe

    query = "CALL gds.beta.pipeline.drop($name)"
    params = {"name": pipe.name()}
    runner.run_query(query, params)


def test_pipeline_list(gds: GraphDataScience, lp_pipe: LPTrainingPipeline) -> None:
    result = gds.beta.pipeline.list()

    assert len(result) == 1
    assert result[0]["pipelineName"] == lp_pipe.name()


def test_model_exists(gds: GraphDataScience) -> None:
    assert not gds.beta.pipeline.exists("NOTHING")["exists"]


def test_model_drop(gds: GraphDataScience) -> None:
    pipe, _ = gds.beta.pipeline.linkPrediction.create(PIPE_NAME)
    assert gds.beta.pipeline.exists(pipe.name())["exists"]

    assert gds.beta.pipeline.drop(pipe)["pipelineName"] == pipe.name()
    assert not gds.beta.pipeline.exists(pipe.name())["exists"]


def test_model_get_lp(gds: GraphDataScience, lp_pipe: LPTrainingPipeline) -> None:
    pipe = gds.pipeline.get(lp_pipe.name())

    assert pipe.name() == lp_pipe.name()
    assert isinstance(pipe, LPTrainingPipeline)
    assert pipe.feature_steps() == lp_pipe.feature_steps()


def test_model_get_nc(gds: GraphDataScience, nc_pipe: NCTrainingPipeline) -> None:
    pipe = gds.pipeline.get(nc_pipe.name())

    assert pipe.name() == nc_pipe.name()
    assert isinstance(pipe, NCTrainingPipeline)
    assert pipe.feature_properties() == nc_pipe.feature_properties()
