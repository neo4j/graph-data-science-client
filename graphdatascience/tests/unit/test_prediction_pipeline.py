import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.model.trained_model import TrainedModel
from graphdatascience.pipeline.lp_prediction_pipeline import LPPredictionPipeline
from graphdatascience.pipeline.nc_prediction_pipeline import NCPredictionPipeline

from .conftest import CollectingQueryRunner

PIPE_NAME = "pipe"


@pytest.fixture(scope="module")
def G(gds: GraphDataScience) -> Graph:
    return gds.graph.project("g", "Node", "REL")


@pytest.fixture
def lp_trained_pipe(gds: GraphDataScience, G: Graph) -> TrainedModel:
    pipe, _ = gds.alpha.ml.pipeline.linkPrediction.create("pipe")
    trainedPipe, _ = pipe.train(G, modelName="m", concurrency=2)
    return trainedPipe


@pytest.fixture
def nc_trained_pipe(gds: GraphDataScience, G: Graph) -> TrainedModel:
    pipe, _ = gds.alpha.ml.pipeline.nodeClassification.create("pipe")
    trainedPipe, _ = pipe.train(G, modelName="m", concurrency=2)
    return trainedPipe


def test_predict_stream_lp_trained_pipeline(
    runner: CollectingQueryRunner, lp_trained_pipe: LPPredictionPipeline, G: Graph
) -> None:
    lp_trained_pipe.predict_stream(G, topN=2)
    assert (
        runner.last_query()
        == "CALL gds.alpha.ml.pipeline.linkPrediction.predict.stream($graph_name, $config)"
    )
    assert runner.last_params() == {
        "graph_name": G.name(),
        "config": {"modelName": lp_trained_pipe.name(), "topN": 2},
    }


def test_predict_mutate_lp_trained_pipeline(
    runner: CollectingQueryRunner, lp_trained_pipe: LPPredictionPipeline, G: Graph
) -> None:
    lp_trained_pipe.predict_mutate(G, topN=2, mutateRelationshipType="HELO")
    assert (
        runner.last_query()
        == "CALL gds.alpha.ml.pipeline.linkPrediction.predict.mutate($graph_name, $config)"
    )
    assert runner.last_params() == {
        "graph_name": G.name(),
        "config": {
            "modelName": lp_trained_pipe.name(),
            "topN": 2,
            "mutateRelationshipType": "HELO",
        },
    }


def test_predict_stream_nc_trained_pipeline(
    runner: CollectingQueryRunner, nc_trained_pipe: NCPredictionPipeline, G: Graph
) -> None:
    nc_trained_pipe.predict_stream(G)
    assert (
        runner.last_query()
        == "CALL gds.alpha.ml.pipeline.nodeClassification.predict.stream($graph_name, $config)"
    )
    assert runner.last_params() == {
        "graph_name": G.name(),
        "config": {"modelName": nc_trained_pipe.name()},
    }


def test_predict_mutate_nc_trained_pipeline(
    runner: CollectingQueryRunner, nc_trained_pipe: NCPredictionPipeline, G: Graph
) -> None:
    nc_trained_pipe.predict_mutate(G, mutateProperty="helo")
    assert (
        runner.last_query()
        == "CALL gds.alpha.ml.pipeline.nodeClassification.predict.mutate($graph_name, $config)"
    )
    assert runner.last_params() == {
        "graph_name": G.name(),
        "config": {
            "modelName": nc_trained_pipe.name(),
            "mutateProperty": "helo",
        },
    }


def test_predict_write_nc_trained_pipeline(
    runner: CollectingQueryRunner, nc_trained_pipe: NCPredictionPipeline, G: Graph
) -> None:
    nc_trained_pipe.predict_write(G, writeProperty="helo")
    assert (
        runner.last_query()
        == "CALL gds.alpha.ml.pipeline.nodeClassification.predict.write($graph_name, $config)"
    )
    assert runner.last_params() == {
        "graph_name": G.name(),
        "config": {
            "modelName": nc_trained_pipe.name(),
            "writeProperty": "helo",
        },
    }
