import pytest

from gdsclient.graph_data_science import GraphDataScience
from gdsclient.pipeline.lp_pipeline import LPPipeline

from .conftest import CollectingQueryRunner

PIPE_NAME = "pipe"


@pytest.fixture
def pipe(gds: GraphDataScience) -> LPPipeline:
    pipe = gds.alpha.ml.pipeline.linkPrediction.create(PIPE_NAME)
    return pipe


def test_create_lp_pipeline(
    runner: CollectingQueryRunner, gds: GraphDataScience
) -> None:
    pipe = gds.alpha.ml.pipeline.linkPrediction.create("hello")
    assert pipe.name() == "hello"

    assert (
        runner.last_query() == "CALL gds.alpha.ml.pipeline.linkPrediction.create($name)"
    )
    assert runner.last_params() == {
        "name": "hello",
    }


def test_add_node_property_lp_pipeline(
    runner: CollectingQueryRunner, pipe: LPPipeline
) -> None:
    pipe.addNodeProperty(
        "pageRank", mutateProperty="rank", dampingFactor=0.2, tolerance=0.3
    )

    assert (
        runner.last_query()
        == "CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty($pipeline_name, $procedure_name, $config)"
    )
    assert runner.last_params() == {
        "pipeline_name": pipe.name(),
        "procedure_name": "pageRank",
        "config": {"mutateProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3},
    }


def test_add_feature_lp_pipeline(
    runner: CollectingQueryRunner, pipe: LPPipeline
) -> None:
    pipe.addFeature("l2", nodeProperties=["prop1"])

    assert (
        runner.last_query()
        == "CALL gds.alpha.ml.pipeline.linkPrediction.addFeature($pipeline_name, $feature_type, $config)"
    )
    assert runner.last_params() == {
        "pipeline_name": pipe.name(),
        "feature_type": "l2",
        "config": {"nodeProperties": ["prop1"]},
    }


def test_configure_split_lp_pipeline(
    runner: CollectingQueryRunner, pipe: LPPipeline
) -> None:
    pipe.configureSplit(trainFraction=0.42)

    assert (
        runner.last_query()
        == "CALL gds.alpha.ml.pipeline.linkPrediction.configureSplit($pipeline_name, $config)"
    )
    assert runner.last_params() == {
        "pipeline_name": pipe.name(),
        "config": {"trainFraction": 0.42},
    }


def test_configure_params_lp_pipeline(
    runner: CollectingQueryRunner, pipe: LPPipeline
) -> None:
    pipe.configureParams([{"tolerance": 0.01}, {"maxEpochs": 500}])

    assert (
        runner.last_query()
        == "CALL gds.alpha.ml.pipeline.linkPrediction.configureParams($pipeline_name, $parameter_space)"
    )
    assert runner.last_params() == {
        "pipeline_name": pipe.name(),
        "parameter_space": [{"tolerance": 0.01}, {"maxEpochs": 500}],
    }


def test_train_lp_pipeline(
    runner: CollectingQueryRunner, gds: GraphDataScience, pipe: LPPipeline
) -> None:
    G = gds.graph.project("g", "*", "*")

    pipe.train(G, modelName="m", concurrency=2)

    assert (
        runner.last_query()
        == "CALL gds.alpha.ml.pipeline.linkPrediction.train($graph_name, $config)"
    )
    assert runner.last_params() == {
        "graph_name": G.name(),
        "config": {"pipeline": pipe.name(), "modelName": "m", "concurrency": 2},
    }
