import pytest

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.pipeline.lp_training_pipeline import LPTrainingPipeline
from graphdatascience.pipeline.training_pipeline import TrainingPipeline

from .conftest import CollectingQueryRunner

PIPELINE_NAME = "dummy"


@pytest.fixture(scope="module")
def pipeline(runner: CollectingQueryRunner) -> TrainingPipeline:
    return LPTrainingPipeline(PIPELINE_NAME, runner)


def test_list_pipelines(runner: CollectingQueryRunner, gds: GraphDataScience, pipeline: TrainingPipeline) -> None:
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


def test_drop_pipeline(runner: CollectingQueryRunner, gds: GraphDataScience, pipeline: TrainingPipeline) -> None:
    gds.alpha.pipeline.drop(pipeline)

    assert runner.last_query() == "CALL gds.alpha.pipeline.drop($pipeline_name)"
    assert runner.last_params() == {"pipeline_name": PIPELINE_NAME}
