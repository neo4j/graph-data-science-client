import pytest

from gdsclient.graph.graph_object import Graph
from gdsclient.graph_data_science import GraphDataScience
from gdsclient.pipeline.lp_trained_pipeline import LPTrainedPipeline
from gdsclient.pipeline.nc_trained_pipeline import NCTrainedPipeline
from gdsclient.pipeline.trained_pipeline import TrainedPipeline

from .conftest import CollectingQueryRunner

PIPE_NAME = "pipe"


@pytest.fixture(scope="module")
def G(gds: GraphDataScience) -> Graph:
    return gds.graph.project("g", "Node", "REL")


@pytest.fixture
def lp_trained_pipe(
    runner: CollectingQueryRunner, gds: GraphDataScience, G: Graph
) -> TrainedPipeline:
    pipe = gds.alpha.ml.pipeline.linkPrediction.create("pipe")
    return pipe.train(G, modelName="m", concurrency=2)


@pytest.fixture
def nc_trained_pipe(
    runner: CollectingQueryRunner, gds: GraphDataScience, G: Graph
) -> TrainedPipeline:
    pipe = gds.alpha.ml.pipeline.nodeClassification.create("pipe")
    return pipe.train(G, modelName="m", concurrency=2)


def test_predict_stream_lp_trained_pipeline(
    runner: CollectingQueryRunner, lp_trained_pipe: LPTrainedPipeline, G: Graph
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
    runner: CollectingQueryRunner, lp_trained_pipe: LPTrainedPipeline, G: Graph
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
    runner: CollectingQueryRunner, nc_trained_pipe: NCTrainedPipeline, G: Graph
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
    runner: CollectingQueryRunner, nc_trained_pipe: NCTrainedPipeline, G: Graph
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
    runner: CollectingQueryRunner, nc_trained_pipe: NCTrainedPipeline, G: Graph
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
