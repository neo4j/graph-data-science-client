from typing import Generator

import pytest

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.pipeline.training_pipeline import TrainingPipeline
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner

PIPE_NAME = "pipe"


@pytest.fixture
def pipe(runner: Neo4jQueryRunner, gds: GraphDataScience) -> Generator[TrainingPipeline, None, None]:
    pipe, _ = gds.beta.pipeline.linkPrediction.create(PIPE_NAME)

    yield pipe

    query = "CALL gds.beta.pipeline.drop($name)"
    params = {"name": pipe.name()}
    runner.run_query(query, params)


def test_pipeline_name(pipe: TrainingPipeline) -> None:
    assert pipe.name() == PIPE_NAME


def test_pipeline_type(pipe: TrainingPipeline) -> None:
    assert pipe.type() == "Link prediction training pipeline"


def test_pipeline_creation_time(pipe: TrainingPipeline) -> None:
    assert pipe.creation_time()


def test_pipeline_exists(pipe: TrainingPipeline) -> None:
    assert pipe.exists()


def test_pipeline_drop(gds: GraphDataScience) -> None:
    pipe, _ = gds.beta.pipeline.linkPrediction.create(PIPE_NAME)

    pipe.drop()
    assert not pipe.exists()
