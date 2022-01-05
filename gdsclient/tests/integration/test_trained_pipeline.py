from typing import Generator

import pytest

from gdsclient.graph.graph_object import Graph
from gdsclient.graph_data_science import GraphDataScience
from gdsclient.pipeline.lp_trained_pipeline import LPTrainedPipeline
from gdsclient.pipeline.nc_trained_pipeline import NCTrainedPipeline
from gdsclient.pipeline.trained_pipeline import TrainedPipeline
from gdsclient.query_runner.neo4j_query_runner import Neo4jQueryRunner


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
    G = gds.graph.project(
        "g", {"Node": {"properties": ["age"]}}, {"REL": {"orientation": "UNDIRECTED"}}
    )

    print(G.relationship_count())

    yield G

    runner.run_query("MATCH (n) DETACH DELETE n")
    G.drop()


@pytest.fixture(scope="module")
def lp_trained_pipe(
    runner: Neo4jQueryRunner, gds: GraphDataScience, G: Graph
) -> Generator[TrainedPipeline, None, None]:
    pipe = gds.alpha.ml.pipeline.linkPrediction.create("pipe")

    try:
        pipe.addNodeProperty("degree", mutateProperty="rank")
        pipe.addFeature("l2", nodeProperties=["rank"])
        pipe.configureSplit(trainFraction=0.4, testFraction=0.2)
        lp_trained_pipe = pipe.train(G, modelName="m", concurrency=2)
    finally:
        query = "CALL gds.beta.model.drop($name)"
        params = {"name": "pipe"}
        runner.run_query(query, params)

    yield lp_trained_pipe

    params = {"name": "m"}
    runner.run_query(query, params)


@pytest.fixture
def nc_trained_pipe(
    runner: Neo4jQueryRunner, gds: GraphDataScience, G: Graph
) -> Generator[TrainedPipeline, None, None]:
    pipe = gds.alpha.ml.pipeline.nodeClassification.create("pipe")

    try:
        pipe.addNodeProperty("degree", mutateProperty="rank")
        pipe.selectFeatures("rank")
        pipe.configureSplit(testFraction=0.3)
        nc_trained_pipe = pipe.train(
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
    lp_trained_pipe: LPTrainedPipeline, G: Graph
) -> None:
    result = lp_trained_pipe.predict_stream(G, topN=2)
    assert len(result) == 2


def test_predict_mutate_lp_trained_pipeline(
    lp_trained_pipe: LPTrainedPipeline, G: Graph
) -> None:
    result = lp_trained_pipe.predict_mutate(
        G, topN=2, mutateRelationshipType="PRED_REL"
    )
    assert result[0]["relationshipsWritten"] == 4


def test_predict_stream_nc_trained_pipeline(
    nc_trained_pipe: NCTrainedPipeline, G: Graph
) -> None:
    result = nc_trained_pipe.predict_stream(G)
    assert len(result) == G.node_count()


def test_predict_mutate_nc_trained_pipeline(
    nc_trained_pipe: NCTrainedPipeline, G: Graph
) -> None:
    result = nc_trained_pipe.predict_mutate(G, mutateProperty="whoa")
    assert result[0]["nodePropertiesWritten"] == G.node_count()
