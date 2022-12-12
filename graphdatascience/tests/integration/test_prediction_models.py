from typing import Generator

import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.model.link_prediction_model import LPModel
from graphdatascience.model.model import Model
from graphdatascience.model.node_classification_model import NCModel
from graphdatascience.model.node_regression_model import NRModel
from graphdatascience.model.pipeline_model import MetricScores
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.server_version.server_version import ServerVersion


@pytest.fixture
def G(runner: Neo4jQueryRunner, gds: GraphDataScience) -> Generator[Graph, None, None]:
    runner.run_query(
        """
        CREATE
        (a: Node {age: 2}),
        (b: Node {age: 3}),
        (c: Node {age: 2}),
        (d: Node {age: 1}),
        (e: Node {age: 2}),
        (i1: CONTEXT {age: 4}),
        (i2: CONTEXT {age: 4}),
        (a)-[:REL]->(b),
        (a)-[:REL]->(c),
        (b)-[:REL]->(c),
        (b)-[:REL]->(a),
        (c)-[:REL]->(a),
        (c)-[:REL]->(b),
        (a)-[:CONTEXTREL]->(i1),
        (b)-[:CONTEXTREL]->(i1),
        (c)-[:CONTEXTREL]->(i1),
        (d)-[:CONTEXTREL]->(i1),
        (e)-[:CONTEXTREL]->(i2)
        """
    )
    G, _ = gds.graph.project(
        "g",
        {"Node": {"properties": ["age"]}, "CONTEXT": {"properties": ["age"]}},
        {"REL": {"orientation": "UNDIRECTED"}, "CONTEXTREL": {"orientation": "UNDIRECTED"}},
    )

    yield G

    runner.run_query("MATCH (n) DETACH DELETE n")
    G.drop()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
@pytest.fixture
def lp_model(runner: Neo4jQueryRunner, gds: GraphDataScience, G: Graph) -> Generator[Model, None, None]:
    pipe, _ = gds.beta.pipeline.linkPrediction.create("pipe")

    try:
        if gds._server_version >= ServerVersion(2, 2, 0):
            pipe.addNodeProperty(
                "degree", mutateProperty="rank", contextNodeLabels=["CONTEXT"], contextRelationshipTypes=["CONTEXTREL"]
            )
        else:
            pipe.addNodeProperty("degree", mutateProperty="rank")
        pipe.addFeature("l2", nodeProperties=["rank"])
        pipe.configureSplit(trainFraction=0.7, testFraction=0.2, validationFolds=2)
        pipe.addLogisticRegression(penalty=1)
        if gds._server_version >= ServerVersion(2, 2, 0):
            lp_model, _ = pipe.train(
                G,
                modelName="lp-model",
                concurrency=2,
                sourceNodeLabel="Node",
                targetNodeLabel="Node",
                targetRelationshipType="REL",
            )
        else:
            lp_model, _ = pipe.train(G, modelName="lp-model", concurrency=2)
    finally:
        query = "CALL gds.beta.pipeline.drop($name)"
        params = {"name": "pipe"}
        runner.run_query(query, params)

    yield lp_model

    query = "CALL gds.beta.model.drop($name)"
    params = {"name": lp_model.name()}
    runner.run_query(query, params)


@pytest.fixture
def nc_model(runner: Neo4jQueryRunner, gds: GraphDataScience, G: Graph) -> Generator[Model, None, None]:
    pipe, _ = gds.beta.pipeline.nodeClassification.create("pipe")

    try:
        if gds._server_version >= ServerVersion(2, 2, 0):
            pipe.addNodeProperty(
                "degree", mutateProperty="rank", contextNodeLabels=["CONTEXT"], contextRelationshipTypes=["CONTEXTREL"]
            )
        else:
            pipe.addNodeProperty("degree", mutateProperty="rank")
        pipe.selectFeatures("rank")
        pipe.configureSplit(testFraction=0.3, validationFolds=2)
        pipe.addLogisticRegression(penalty=1)
        if gds._server_version >= ServerVersion(2, 2, 0):
            nc_model, _ = pipe.train(
                G,
                modelName="nc-model",
                targetNodeLabels=["Node"],
                relationshipTypes=["REL"],
                targetProperty="age",
                metrics=["ACCURACY"],
            )
        else:
            nc_model, _ = pipe.train(G, modelName="nc-model", targetProperty="age", metrics=["ACCURACY"])
    finally:
        query = "CALL gds.beta.pipeline.drop($name)"
        params = {"name": "pipe"}
        runner.run_query(query, params)

    yield nc_model

    query = "CALL gds.beta.model.drop($name)"
    params = {"name": nc_model.name()}
    runner.run_query(query, params)


@pytest.fixture
def nr_model(runner: Neo4jQueryRunner, gds: GraphDataScience, G: Graph) -> Generator[Model, None, None]:
    pipe, _ = gds.alpha.pipeline.nodeRegression.create("pipe")

    try:
        if gds._server_version >= ServerVersion(2, 2, 0):
            pipe.addNodeProperty(
                "degree", mutateProperty="rank", contextNodeLabels=["CONTEXT"], contextRelationshipTypes=["CONTEXTREL"]
            )
        else:
            pipe.addNodeProperty("degree", mutateProperty="rank")
        pipe.selectFeatures("rank")
        pipe.configureSplit(testFraction=0.3, validationFolds=2)
        pipe.addLinearRegression(penalty=1)
        if gds._server_version >= ServerVersion(2, 2, 0):
            nr_model, _ = pipe.train(
                G,
                modelName="nr_model",
                targetNodeLabels=["Node"],
                relationshipTypes=["REL"],
                targetProperty="age",
                metrics=["MEAN_SQUARED_ERROR"],
            )
        else:
            nr_model, _ = pipe.train(G, modelName="nr-model", targetProperty="age", metrics=["MEAN_SQUARED_ERROR"])
    finally:
        query = "CALL gds.beta.pipeline.drop($name)"
        params = {"name": "pipe"}
        runner.run_query(query, params)

    yield nr_model

    query = "CALL gds.beta.model.drop($name)"
    params = {"name": nr_model.name()}
    runner.run_query(query, params)


def test_predict_stream_lp_model(lp_model: LPModel, G: Graph) -> None:
    result = lp_model.predict_stream(G, topN=2)
    assert len(result) == 2


def test_predict_mutate_lp_model(lp_model: LPModel, G: Graph) -> None:
    result = lp_model.predict_mutate(G, topN=2, mutateRelationshipType="PRED_REL")
    assert result["relationshipsWritten"] == 4


def test_estimate_predict_stream_nc_model(gds: GraphDataScience, nc_model: NCModel, G: Graph) -> None:
    if gds._server_version >= ServerVersion(2, 2, 0):
        result = nc_model.predict_stream_estimate(G, targetNodeLabels=["CONTEXT"])
    else:
        result = nc_model.predict_stream_estimate(G, nodeLabels=["CONTEXT"])
    assert result["requiredMemory"]


def test_predict_stream_nc_model(gds: GraphDataScience, nc_model: NCModel, G: Graph) -> None:
    if gds._server_version >= ServerVersion(2, 2, 0):
        result = nc_model.predict_stream(G, targetNodeLabels=["CONTEXT"])
    else:
        result = nc_model.predict_stream(G, nodeLabels=["CONTEXT"])
    assert len(result) == 2


def test_predict_mutate_nc_model(gds: GraphDataScience, nc_model: NCModel, G: Graph) -> None:
    if gds._server_version >= ServerVersion(2, 2, 0):
        result = nc_model.predict_mutate(G, mutateProperty="nc_mutate", targetNodeLabels=["CONTEXT"])
    else:
        result = nc_model.predict_mutate(G, mutateProperty="nc_mutate", nodeLabels=["CONTEXT"])
    assert result["nodePropertiesWritten"] == 2


def test_predict_write_nc_model(gds: GraphDataScience, nc_model: NCModel, G: Graph) -> None:
    if gds._server_version >= ServerVersion(2, 2, 0):
        result = nc_model.predict_write(G, writeProperty="nc_mutate", targetNodeLabels=["CONTEXT"])
    else:
        result = nc_model.predict_write(G, writeProperty="nc_mutate", nodeLabels=["CONTEXT"])
    assert result["nodePropertiesWritten"] == 2


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_predict_stream_nr_model(gds: GraphDataScience, nr_model: NRModel, G: Graph) -> None:
    if gds._server_version >= ServerVersion(2, 2, 0):
        result = nr_model.predict_stream(G, targetNodeLabels=["CONTEXT"])
    else:
        result = nr_model.predict_stream(G, nodeLabels=["CONTEXT"])
    assert len(result) == 2


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_predict_mutate_nr_model(gds: GraphDataScience, nr_model: NRModel, G: Graph) -> None:
    if gds._server_version >= ServerVersion(2, 2, 0):
        result = nr_model.predict_mutate(G, mutateProperty="nr_mutate", targetNodeLabels=["CONTEXT"])
    else:
        result = nr_model.predict_mutate(G, mutateProperty="nr_mutate", nodeLabels=["CONTEXT"])

    assert result["nodePropertiesWritten"] == 2


def test_type_nc_model(nc_model: NCModel) -> None:
    assert nc_model.type() == "NodeClassification"


def test_train_config_nc_model(nc_model: NCModel, G: Graph) -> None:
    train_config = nc_model.train_config()
    assert train_config["modelName"] == nc_model.name()
    assert train_config["graphName"] == G.name()


def test_graph_schema_nc_model(
    nc_model: NCModel,
) -> None:
    graph_schema = nc_model.graph_schema()
    assert "nodes" in graph_schema.keys()


def test_loaded_nc_model(nc_model: NCModel) -> None:
    assert nc_model.loaded()


def test_stored_nc_model(nc_model: NCModel) -> None:
    assert not nc_model.stored()


def test_creation_time_nc_model(
    nc_model: NCModel,
) -> None:
    assert nc_model.creation_time()


def test_shared_nc_model(nc_model: NCModel) -> None:
    assert not nc_model.shared()


def test_metrics_nc_model(nc_model: NCModel) -> None:
    metrics = nc_model.metrics()

    assert "ACCURACY" in metrics.keys()
    assert isinstance(metrics["ACCURACY"], MetricScores)


def test_metrics_nr_model(nr_model: NRModel) -> None:
    metrics = nr_model.metrics()

    assert "MEAN_SQUARED_ERROR" in metrics.keys()
    assert isinstance(metrics["MEAN_SQUARED_ERROR"], MetricScores)


def test_metrics_lp_model(lp_model: LPModel) -> None:
    metrics = nc_model.metrics()

    assert "AUCPR" in metrics.keys()
    assert isinstance(metrics["AUCPR"], MetricScores)


def test_best_parameters_nc_model(nc_model: NCModel) -> None:
    assert nc_model.best_parameters()["methodName"] == "LogisticRegression"


def test_pipeline_nc_model(nc_model: NCModel) -> None:
    assert len(nc_model.pipeline()["nodePropertySteps"]) > 0
