from typing import Generator

import pytest

from gdsclient.graph_data_science import GraphDataScience
from gdsclient.pipeline.nc_training_pipeline import NCTrainingPipeline
from gdsclient.query_runner.neo4j_query_runner import Neo4jQueryRunner

PIPE_NAME = "p"


@pytest.fixture
def nc_pipe(
    runner: Neo4jQueryRunner, gds: GraphDataScience
) -> Generator[NCTrainingPipeline, None, None]:
    pipe = gds.alpha.ml.pipeline.nodeClassification.create(PIPE_NAME)

    yield pipe

    query = "CALL gds.beta.model.drop($name)"
    params = {"name": pipe.name()}
    runner.run_query(query, params)


def test_model_exists(nc_pipe: NCTrainingPipeline) -> None:
    assert nc_pipe.exists()


def test_model_drop(gds: GraphDataScience) -> None:
    pipe = gds.alpha.ml.pipeline.nodeClassification.create(PIPE_NAME)

    pipe.drop()
    assert not pipe.exists()
