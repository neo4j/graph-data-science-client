from typing import Generator

import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.model.trained_model import TrainedModel
from graphdatascience.pipeline.lp_prediction_pipeline import LPPredictionPipeline
from graphdatascience.pipeline.nc_prediction_pipeline import NCPredictionPipeline
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


@pytest.fixture(scope="module")
def G(runner: Neo4jQueryRunner, gds: GraphDataScience) -> Generator[Graph, None, None]:
    runner.run_query(
        """
        CREATE
        (a: Node {age: 2}),
        (b: Node {age: 3}),
        (c: Node {age: 2}),
        (d: Node {age: 1}),
        (e: Node {age: 2}),
        (a)-[:REL]->(b),
        (a)-[:REL]->(c),
        (b)-[:REL]->(c),
        (b)-[:REL]->(a),
        (c)-[:REL]->(a),
        (c)-[:REL]->(b)
        """
    )
    G, _ = gds.graph.project(
        "g", {"Node": {"properties": ["age"]}}, {"REL": {"orientation": "UNDIRECTED"}}
    )

    yield G

    runner.run_query("MATCH (n) DETACH DELETE n")
    G.drop()


@pytest.fixture(scope="module")
def lp_trained_pipe(
    runner: Neo4jQueryRunner, gds: GraphDataScience, G: Graph
) -> Generator[TrainedModel, None, None]:
    pipe, _ = gds.alpha.ml.pipeline.linkPrediction.create("pipe")

    try:
        pipe.addNodeProperty("degree", mutateProperty="rank")
        pipe.addFeature("l2", nodeProperties=["rank"])
        pipe.configureSplit(trainFraction=0.4, testFraction=0.2)
        lp_trained_pipe, _ = pipe.train(G, modelName="m", concurrency=2)
    finally:
        query = "CALL gds.beta.model.drop($name)"
        params = {"name": "pipe"}
        runner.run_query(query, params)

    yield lp_trained_pipe

    params = {"name": "m"}
    runner.run_query(query, params)


@pytest.fixture(scope="module")
def nc_trained_pipe(
    runner: Neo4jQueryRunner, gds: GraphDataScience, G: Graph
) -> Generator[TrainedModel, None, None]:
    pipe, _ = gds.alpha.ml.pipeline.nodeClassification.create("pipe")

    try:
        pipe.addNodeProperty("degree", mutateProperty="rank")
        pipe.selectFeatures("rank")
        pipe.configureSplit(testFraction=0.3)
        nc_trained_pipe, _ = pipe.train(
            G, modelName="n", targetProperty="age", metrics=["ACCURACY"]
        )
    finally:
        query = "CALL gds.beta.model.drop($name)"
        params = {"name": "pipe"}
        runner.run_query(query, params)

    yield nc_trained_pipe

    params = {"name": "n"}
    runner.run_query(query, params)


def test_predict_stream_lp_trained_pipeline(
    lp_trained_pipe: LPPredictionPipeline, G: Graph
) -> None:
    result = lp_trained_pipe.predict_stream(G, topN=2)
    assert len(result) == 2


def test_predict_mutate_lp_trained_pipeline(
    lp_trained_pipe: LPPredictionPipeline, G: Graph
) -> None:
    result = lp_trained_pipe.predict_mutate(
        G, topN=2, mutateRelationshipType="PRED_REL"
    )
    assert result["relationshipsWritten"] == 4


def test_predict_stream_nc_trained_pipeline(
    nc_trained_pipe: NCPredictionPipeline, G: Graph
) -> None:
    result = nc_trained_pipe.predict_stream(G)
    assert len(result) == G.node_count()


def test_predict_mutate_nc_trained_pipeline(
    nc_trained_pipe: NCPredictionPipeline, G: Graph
) -> None:
    result = nc_trained_pipe.predict_mutate(G, mutateProperty="whoa")
    assert result["nodePropertiesWritten"] == G.node_count()


def test_predict_write_nc_trained_pipeline(
    nc_trained_pipe: NCPredictionPipeline, G: Graph
) -> None:
    result = nc_trained_pipe.predict_write(G, writeProperty="whoa")
    assert result["nodePropertiesWritten"] == G.node_count()


def test_type_nc_trained_pipeline(nc_trained_pipe: NCPredictionPipeline) -> None:
    assert nc_trained_pipe.type() == "Node classification pipeline"


def test_train_config_nc_trained_pipeline(
    nc_trained_pipe: NCPredictionPipeline, G: Graph
) -> None:
    train_config = nc_trained_pipe.train_config()
    assert train_config["modelName"] == nc_trained_pipe.name()
    assert train_config["graphName"] == G.name()


def test_graph_schema_nc_trained_pipeline(
    nc_trained_pipe: NCPredictionPipeline,
) -> None:
    graph_schema = nc_trained_pipe.graph_schema()
    assert "nodes" in graph_schema.keys()


def test_loaded_nc_trained_pipeline(nc_trained_pipe: NCPredictionPipeline) -> None:
    assert nc_trained_pipe.loaded()


def test_stored_nc_trained_pipeline(nc_trained_pipe: NCPredictionPipeline) -> None:
    assert not nc_trained_pipe.stored()


def test_creation_time_nc_trained_pipeline(
    nc_trained_pipe: NCPredictionPipeline,
) -> None:
    assert nc_trained_pipe.creation_time()


def test_shared_nc_trained_pipeline(nc_trained_pipe: NCPredictionPipeline) -> None:
    assert not nc_trained_pipe.shared()


def test_metrics_nc_trained_pipeline(nc_trained_pipe: NCPredictionPipeline) -> None:
    assert "ACCURACY" in nc_trained_pipe.metrics().keys()
