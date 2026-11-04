from collections import OrderedDict
from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.node_embedding.graphsage_supervised_model import (
    GraphSageSupervisedModel,
)
from graphdatascience.procedure_surface.arrow.model.model_catalog_arrow_endpoints import ModelCatalogArrowEndpoints
from graphdatascience.procedure_surface.arrow.node_embedding.graphsage_supervised_arrow_endpoints import (
    GraphSageSupervisedArrowEndpoints,
)
from graphdatascience.query_runner import QueryRunner, QueryType
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol
from tests.integration.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

graph = """
    CREATE
    (a: Node {feature: 1.0, label: 0}),
    (b: Node {feature: 2.0, label: 1}),
    (c: Node {feature: 3.0, label: 0}),
    (d: Node {feature: 4.0, label: 1}),
    (a)-[:REL]->(b),
    (b)-[:REL]->(c),
    (c)-[:REL]->(d),
    (d)-[:REL]->(a)
    """


@pytest.fixture
def sample_graph(arrow_client_runtime: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    with create_graph(arrow_client_runtime, "gs-sup-g", graph) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client_runtime: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[Graph, None, None]:
    with create_graph_from_db(
        arrow_client_runtime,
        query_runner,
        "gs-sup-g",
        graph,
        """
            MATCH (n)-[r]->(m)
            WITH gds.graph.project.remote(n, m, {
                sourceNodeLabels: labels(n),
                targetNodeLabels: labels(m),
                sourceNodeProperties: properties(n),
                targetNodeProperties: properties(m),
                relationshipType: type(r)
            }) as g
            RETURN g
        """,
    ) as g:
        yield g


@pytest.fixture(scope="module")
def training_graph(arrow_client_runtime: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    """Graph the shared model is trained on; distinct name so it can coexist with `sample_graph`."""
    with create_graph(arrow_client_runtime, "gs-sup-train-g", graph) as G:
        yield G


@pytest.fixture(scope="module")
def gs_model(
    arrow_client_runtime: AuthenticatedArrowClient, training_graph: Graph
) -> Generator[GraphSageSupervisedModel, None, None]:
    # Trained once per module: training spawns a python-runtime container, and prediction
    # does not mutate the model, so every test can share it.
    model_name = "gs-sup-model"
    model_catalog = ModelCatalogArrowEndpoints(arrow_client_runtime)
    try:
        endpoints = GraphSageSupervisedArrowEndpoints(arrow_client_runtime, None, show_progress=False)
        model, _ = endpoints.train(
            G=training_graph,
            model_name=model_name,
            feature_properties=["feature"],
            target_label="Node",
            target_property="label",
            embedding_dimension=1,
            epochs=1,
        )

        yield model
    finally:
        # `drop` only unloads the model; `delete` removes the stored artifact so it cannot
        # collide with the next run's training under the same name.
        model_catalog.drop(model_name, fail_if_missing=False)
        model_catalog.delete(model_name, fail_if_missing=False)


def test_train(gs_model: GraphSageSupervisedModel) -> None:
    assert gs_model.name() == "gs-sup-model"
    assert gs_model.exists()


def test_stream(gs_model: GraphSageSupervisedModel, sample_graph: Graph) -> None:
    result = gs_model.predict_stream(sample_graph, feature_properties=["feature"])

    assert set(result.columns) == {"nodeId", "predictedClass", "predictedProbabilities"}
    assert len(result) == 4


def test_compute_stream(
    arrow_client_runtime: AuthenticatedArrowClient, gs_model: GraphSageSupervisedModel, sample_graph: Graph
) -> None:
    endpoints = GraphSageSupervisedArrowEndpoints(arrow_client_runtime, None, show_progress=False)

    handle = endpoints.compute(G=sample_graph, model_name=gs_model.name(), feature_properties=["feature"])
    handle.wait()

    result = handle.stream()

    assert set(result.columns) == {"nodeId", "predictedClass", "predictedProbabilities"}
    assert len(result) == 4


def test_compute_mutate(
    arrow_client_runtime: AuthenticatedArrowClient, gs_model: GraphSageSupervisedModel, sample_graph: Graph
) -> None:
    endpoints = GraphSageSupervisedArrowEndpoints(arrow_client_runtime, None, show_progress=False)

    handle = endpoints.compute(G=sample_graph, model_name=gs_model.name(), feature_properties=["feature"])
    handle.wait()

    result = handle.mutate(
        mutate_property_overwrites=OrderedDict(
            [("predicted_class", "predictedClass"), ("predicted_probabilities", "predictedProbabilities")]
        )
    )

    # two properties (predicted classes + probabilities) written for each of the 4 nodes
    assert result["nodePropertiesWritten"] == 8


def test_mutate(gs_model: GraphSageSupervisedModel, sample_graph: Graph) -> None:
    result = gs_model.predict_mutate(
        sample_graph,
        feature_properties=["feature"],
        mutate_property="predictedClass",
        predicted_probability_property="predictedProbabilities",
    )

    # two properties (predicted classes + probabilities) written for each of the 4 nodes
    assert result.node_properties_written == 8
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0


@pytest.mark.db_integration
def test_write(
    arrow_client_runtime: AuthenticatedArrowClient,
    query_runner: QueryRunner,
    db_graph: Graph,
    gs_model: GraphSageSupervisedModel,
) -> None:
    # The shared model is trained without a write protocol, so bind one here to write to the db.
    endpoints = GraphSageSupervisedArrowEndpoints(
        arrow_client_runtime, WriteProtocol.select(arrow_client_runtime, query_runner), show_progress=False
    )

    result = endpoints.write(
        G=db_graph,
        model_name=gs_model.name(),
        feature_properties=["feature"],
        write_property="predictedClass",
        predicted_probability_property="predictedProbabilities",
    )

    # two properties (predicted classes + probabilities) written for each of the 4 nodes
    assert result.node_properties_written == 8
    assert result.compute_millis >= 0
    assert result.write_millis >= 0

    assert (
        query_runner.run_cypher(
            "MATCH (n) WHERE n.predictedClass IS NOT NULL RETURN COUNT(*) AS count",
            query_type=QueryType.USER_ACTION,
        ).iloc[0, 0]
        == 4
    )
