from typing import Generator

import pytest

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.pipeline.lp_training_pipeline import LPTrainingPipeline
from graphdatascience.pipeline.nc_training_pipeline import NCTrainingPipeline
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.server_version.server_version import ServerVersion

PIPE_NAME = "pipe"


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


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_pipeline_list(gds: GraphDataScience, lp_pipe: LPTrainingPipeline) -> None:
    result = gds.pipeline.list()

    assert len(result) == 1
    assert result["pipelineName"][0] == lp_pipe.name()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_pipeline_exists(gds: GraphDataScience) -> None:
    assert not gds.pipeline.exists("NOTHING")["exists"]


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_pipeline_drop(gds: GraphDataScience) -> None:
    pipe, _ = gds.beta.pipeline.linkPrediction.create(PIPE_NAME)
    assert gds.pipeline.exists(pipe.name())["exists"]

    assert gds.pipeline.drop(pipe)["pipelineName"] == pipe.name()
    assert not gds.pipeline.exists(pipe.name())["exists"]


@pytest.mark.compatible_with(max_exclusive=ServerVersion(2, 5, 0))
def test_pipeline_beta_endpoints(gds: GraphDataScience) -> None:
    pipe = gds.lp_pipe(PIPE_NAME)

    assert gds.beta.pipeline.exists(pipe.name())["exists"]
    assert gds.beta.pipeline.list(pipe)["pipelineName"][0] == pipe.name()
    assert gds.beta.pipeline.drop(pipe)["pipelineName"] == pipe.name()


def test_pipeline_get_lp(gds: GraphDataScience, lp_pipe: LPTrainingPipeline) -> None:
    pipe = gds.pipeline.get(lp_pipe.name())

    assert pipe.name() == lp_pipe.name()
    assert isinstance(pipe, LPTrainingPipeline)
    assert pipe.feature_steps().empty


def test_pipeline_get_nc(gds: GraphDataScience, nc_pipe: NCTrainingPipeline) -> None:
    pipe = gds.pipeline.get(nc_pipe.name())

    assert pipe.name() == nc_pipe.name()
    assert isinstance(pipe, NCTrainingPipeline)
    assert pipe.feature_properties().empty
