from typing import Generator

import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.model.graphsage_model import GraphSageModel
from graphdatascience.model.link_prediction_model import LPModel
from graphdatascience.model.model import Model
from graphdatascience.model.node_classification_model import NCModel
from graphdatascience.model.node_regression_model import NRModel
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.server_version.server_version import ServerVersion
from tests.integration.conftest import is_neo4j_44

PIPE_NAME = "pipe"


@pytest.fixture
def G(runner: Neo4jQueryRunner, gds: GraphDataScience) -> Generator[Graph, None, None]:
    runner.run_cypher(
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

    runner.run_cypher("MATCH (n) DETACH DELETE n")
    G.drop()


@pytest.fixture
def lp_model(runner: Neo4jQueryRunner, gds: GraphDataScience, G: Graph) -> Generator[Model, None, None]:
    pipe, _ = gds.beta.pipeline.linkPrediction.create("pipelp")

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
        pipe.drop()

    yield lp_model

    lp_model.drop()


@pytest.fixture
def nc_model(runner: Neo4jQueryRunner, gds: GraphDataScience, G: Graph) -> Generator[Model, None, None]:
    pipe, _ = gds.beta.pipeline.nodeClassification.create("pipenc")

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
        pipe.drop()

    yield nc_model

    nc_model.drop()


@pytest.fixture
def nr_model(runner: Neo4jQueryRunner, gds: GraphDataScience, G: Graph) -> Generator[Model, None, None]:
    pipe, _ = gds.alpha.pipeline.nodeRegression.create("pipenr")

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
            nr_model, _ = pipe.train(G, modelName="nr_model", targetProperty="age", metrics=["MEAN_SQUARED_ERROR"])
    finally:
        pipe.drop()

    yield nr_model

    nr_model.drop()


@pytest.fixture
def gs_model(runner: Neo4jQueryRunner, gds: GraphDataScience, G: Graph) -> Generator[GraphSageModel, None, None]:
    model, _ = gds.beta.graphSage.train(G, modelName="gs-model", featureProperties=["age"])

    yield model

    namespace = "beta." if gds.server_version() < ServerVersion(2, 5, 0) else ""
    query = f"CALL gds.{namespace}model.drop($name, false)"
    params = {"name": model.name()}
    runner.run_cypher(query, params)


@pytest.mark.model_store_location
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_beta_alpha_endpoints_25(gds: GraphDataScience, lp_model: LPModel) -> None:
    if not is_neo4j_44(gds):
        with pytest.warns(DeprecationWarning):
            gds.beta.model.list(lp_model)
            gds.beta.model.exists(lp_model.name())
            gds.alpha.model.store(lp_model)
            gds.beta.model.drop(lp_model)
            gds.alpha.model.load(lp_model.name())
            gds.alpha.model.publish(lp_model)


@pytest.mark.model_store_location
@pytest.mark.compatible_with(max_exclusive=ServerVersion(2, 5, 0))
def test_beta_alpha_endpoints_24(gds: GraphDataScience, lp_model: LPModel) -> None:
    gds.beta.model.list(lp_model)
    gds.beta.model.exists(lp_model.name())
    gds.alpha.model.store(lp_model)
    gds.beta.model.drop(lp_model)
    gds.alpha.model.load(lp_model.name())
    gds.alpha.model.publish(lp_model)


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_model_list(gds: GraphDataScience, lp_model: LPModel) -> None:
    result = gds.model.list()

    assert len(result) == 1
    assert result["modelName"][0] == lp_model.name()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_model_exists(gds: GraphDataScience) -> None:
    assert not gds.model.exists("NOTHING")["exists"]


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
@pytest.mark.enterprise
def test_model_publish(runner: Neo4jQueryRunner, gds: GraphDataScience, gs_model: GraphSageModel) -> None:
    assert not gs_model.shared()

    shared_model = gds.model.publish(gs_model)

    assert shared_model.shared()
    assert isinstance(shared_model, GraphSageModel)

    namespace = "beta." if gds.server_version() < ServerVersion(2, 5, 0) else ""
    query = f"CALL gds.{namespace}model.drop($name)"
    params = {"name": shared_model.name()}
    runner.run_cypher(query, params)


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
@pytest.mark.model_store_location
def test_model_load(runner: Neo4jQueryRunner, gds: GraphDataScience, gs_model: GraphSageModel) -> None:
    runner.run_cypher(f"CALL gds.model.store('{gs_model.name()}')")
    runner.run_cypher(f"CALL gds.model.drop('{gs_model.name()}')")

    model, result = gds.model.load(gs_model.name())
    assert isinstance(model, GraphSageModel)
    assert result["loadMillis"] >= 0

    runner.run_cypher(f"CALL gds.model.drop('{gs_model.name()}')")
    runner.run_cypher(f"CALL gds.model.delete('{model.name()}')")


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
@pytest.mark.model_store_location
def test_model_store(runner: Neo4jQueryRunner, gds: GraphDataScience, gs_model: GraphSageModel) -> None:
    model_name = gds.model.store(gs_model)["modelName"]

    # Should be deletable now
    runner.run_cypher(f"CALL gds.model.drop('{gs_model.name()}')")
    runner.run_cypher(f"CALL gds.model.delete('{model_name}')")


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
@pytest.mark.model_store_location
def test_model_delete(runner: Neo4jQueryRunner, gds: GraphDataScience, gs_model: GraphSageModel) -> None:
    model_name = runner.run_cypher(f"CALL gds.model.store('{gs_model.name()}')")["modelName"][0]

    model = gds.model.get(model_name)
    runner.run_cypher(f"CALL gds.model.drop('{gs_model.name()}')")
    assert gds.model.delete(model)["deleteMillis"] >= 0

    res = runner.run_cypher(f"CALL gds.model.exists('{model_name}')")
    assert not res["exists"][0]


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_model_drop(gds: GraphDataScience, G: Graph) -> None:
    model, _ = gds.beta.graphSage.train(G, modelName="hello", featureProperties=["age"])

    assert gds.model.exists(model.name())["exists"]

    assert gds.model.drop(model)["modelName"] == model.name()
    assert not gds.model.exists(model.name())["exists"]


def test_model_get_lp_trained(gds: GraphDataScience, lp_model: LPModel) -> None:
    new_model = gds.model.get(lp_model.name())

    assert new_model.type() == lp_model.type()
    assert new_model.name() == lp_model.name()
    assert isinstance(new_model, LPModel)


def test_model_classes_nc_trained(nc_model: NCModel) -> None:
    assert nc_model.classes() == [1, 2, 3]


def test_model_get_nc_trained(gds: GraphDataScience, nc_model: NCModel) -> None:
    new_model = gds.model.get(nc_model.name())

    assert new_model.type() == nc_model.type()
    assert new_model.name() == nc_model.name()
    assert isinstance(new_model, NCModel)


def test_model_get_nr_trained(gds: GraphDataScience, nr_model: NRModel) -> None:
    new_model = gds.model.get(nr_model.name())

    assert new_model.type() == nr_model.type()
    assert new_model.name() == nr_model.name()
    assert isinstance(new_model, NRModel)


def test_model_get_graphsage(gds: GraphDataScience, gs_model: GraphSageModel) -> None:
    model = gds.model.get(gs_model.name())

    assert model.type() == gs_model.type()
    assert model.name() == gs_model.name()
    assert isinstance(model, GraphSageModel)

    model.drop()


def test_model_get_no_model(gds: GraphDataScience) -> None:
    with pytest.raises(ValueError, match="No loaded model named 'no_model' exists"):
        gds.model.get("no_model")


def test_missing_model_info(gds: GraphDataScience) -> None:
    model = GraphSageModel("ghost-model", gds._query_runner, gds.server_version())
    with pytest.raises(ValueError, match="There is no 'ghost-model' in the model catalog"):
        model.model_info()


def test_missing_model_drop(gds: GraphDataScience) -> None:
    model = GraphSageModel("ghost-model", gds._query_runner, gds.server_version())

    assert model.drop(failIfMissing=False).empty


@pytest.mark.model_store_location
@pytest.mark.filterwarnings("ignore: The query used a deprecated procedure.")
@pytest.mark.filterwarnings("ignore: .*feature deprecated with replacement")
def test_deprecated_model_Functions_still_work(gds: GraphDataScience, gs_model: GraphSageModel) -> None:
    gds.beta.model.list()
    gds.alpha.model.store(gs_model)
    gds.beta.model.drop(gs_model)
    gds.alpha.model.load(gs_model.name())
    gds.alpha.model.delete(gs_model)
