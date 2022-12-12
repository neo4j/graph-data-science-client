import pytest

from .conftest import CollectingQueryRunner
from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.model.link_prediction_model import LPModel
from graphdatascience.model.model import Model
from graphdatascience.model.node_classification_model import NCModel
from graphdatascience.model.node_regression_model import NRModel

PIPE_NAME = "pipe"


@pytest.fixture
def G(gds: GraphDataScience) -> Graph:
    G_, _ = gds.graph.project("g", "Node", "REL")
    return G_


@pytest.fixture
def lp_model(gds: GraphDataScience, G: Graph) -> Model:
    pipe, _ = gds.beta.pipeline.linkPrediction.create("pipe")
    trained_pipe, _ = pipe.train(G, modelName="m", concurrency=2)
    return trained_pipe


@pytest.fixture
def nc_model(gds: GraphDataScience, G: Graph) -> Model:
    pipe, _ = gds.beta.pipeline.nodeClassification.create("pipe")
    trained_pipe, _ = pipe.train(G, modelName="m", concurrency=2)
    return trained_pipe


@pytest.fixture
def nr_model(gds: GraphDataScience, G: Graph) -> Model:
    pipe, _ = gds.alpha.pipeline.nodeRegression.create("pipe")
    trained_pipe, _ = pipe.train(G, modelName="m", concurrency=2)
    return trained_pipe


def test_predict_stream_lp_model(runner: CollectingQueryRunner, lp_model: LPModel, G: Graph) -> None:
    lp_model.predict_stream(G, topN=2)
    assert runner.last_query() == "CALL gds.beta.pipeline.linkPrediction.predict.stream($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": G.name(),
        "config": {"modelName": lp_model.name(), "topN": 2},
    }


def test_predict_mutate_lp_model(runner: CollectingQueryRunner, lp_model: LPModel, G: Graph) -> None:
    lp_model.predict_mutate(G, topN=2, mutateRelationshipType="HELO")
    assert runner.last_query() == "CALL gds.beta.pipeline.linkPrediction.predict.mutate($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": G.name(),
        "config": {
            "modelName": lp_model.name(),
            "topN": 2,
            "mutateRelationshipType": "HELO",
        },
    }


def test_estimate_predict_stream_nc_model(runner: CollectingQueryRunner, nc_model: NCModel, G: Graph) -> None:
    nc_model.predict_stream_estimate(G)

    assert (
        runner.last_query() == "CALL gds.beta.pipeline.nodeClassification.predict.stream.estimate($graph_name, $config)"
    )
    assert runner.last_params() == {
        "graph_name": G.name(),
        "config": {"modelName": nc_model.name()},
    }


def test_predict_stream_nc_model(runner: CollectingQueryRunner, nc_model: NCModel, G: Graph) -> None:
    nc_model.predict_stream(G)
    assert runner.last_query() == "CALL gds.beta.pipeline.nodeClassification.predict.stream($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": G.name(),
        "config": {"modelName": nc_model.name()},
    }


def test_estimate_predict_write_nc_model(runner: CollectingQueryRunner, nc_model: NCModel, G: Graph) -> None:
    nc_model.predict_write_estimate(G)

    assert (
        runner.last_query() == "CALL gds.beta.pipeline.nodeClassification.predict.write.estimate($graph_name, $config)"
    )
    assert runner.last_params() == {
        "graph_name": G.name(),
        "config": {"modelName": nc_model.name()},
    }


def test_predict_mutate_nc_model(runner: CollectingQueryRunner, nc_model: NCModel, G: Graph) -> None:
    nc_model.predict_mutate(G, mutateProperty="helo")
    assert runner.last_query() == "CALL gds.beta.pipeline.nodeClassification.predict.mutate($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": G.name(),
        "config": {
            "modelName": nc_model.name(),
            "mutateProperty": "helo",
        },
    }


def test_estimate_predict_mutate_nc_model(runner: CollectingQueryRunner, nc_model: NCModel, G: Graph) -> None:
    nc_model.predict_mutate_estimate(G)

    assert (
        runner.last_query() == "CALL gds.beta.pipeline.nodeClassification.predict.mutate.estimate($graph_name, $config)"
    )
    assert runner.last_params() == {
        "graph_name": G.name(),
        "config": {"modelName": nc_model.name()},
    }


def test_predict_write_nc_model(runner: CollectingQueryRunner, nc_model: NCModel, G: Graph) -> None:
    nc_model.predict_write(G, writeProperty="helo")
    assert runner.last_query() == "CALL gds.beta.pipeline.nodeClassification.predict.write($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": G.name(),
        "config": {
            "modelName": nc_model.name(),
            "writeProperty": "helo",
        },
    }


def test_predict_stream_nr_model(runner: CollectingQueryRunner, nr_model: NRModel, G: Graph) -> None:
    nr_model.predict_stream(G)
    assert runner.last_query() == "CALL gds.alpha.pipeline.nodeRegression.predict.stream($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": G.name(),
        "config": {"modelName": nr_model.name()},
    }
