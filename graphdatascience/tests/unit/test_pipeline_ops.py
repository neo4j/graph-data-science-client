import pytest

from .conftest import CollectingQueryRunner
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.pipeline.lp_training_pipeline import LPTrainingPipeline
from graphdatascience.server_version.server_version import ServerVersion

PIPELINE_NAME = "dummy"


@pytest.fixture
def pipeline(runner: CollectingQueryRunner, server_version: ServerVersion) -> LPTrainingPipeline:
    return LPTrainingPipeline(PIPELINE_NAME, runner, server_version)


def test_list_pipelines(runner: CollectingQueryRunner, gds: GraphDataScience, pipeline: LPTrainingPipeline) -> None:
    gds.beta.pipeline.list(pipeline)

    assert runner.last_query() == "CALL gds.beta.pipeline.list($pipeline_name)"
    assert runner.last_params() == {"pipeline_name": PIPELINE_NAME}

    gds.beta.pipeline.list()

    assert runner.last_query() == "CALL gds.beta.pipeline.list()"
    assert runner.last_params() == {}


def test_exists_pipeline(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.alpha.pipeline.exists("my_pipeline")

    assert runner.last_query() == "CALL gds.alpha.pipeline.exists($pipeline_name)"
    assert runner.last_params() == {"pipeline_name": "my_pipeline"}


def test_drop_pipeline(runner: CollectingQueryRunner, gds: GraphDataScience, pipeline: LPTrainingPipeline) -> None:
    gds.alpha.pipeline.drop(pipeline)

    assert runner.last_query() == "CALL gds.alpha.pipeline.drop($pipeline_name)"
    assert runner.last_params() == {"pipeline_name": PIPELINE_NAME}
