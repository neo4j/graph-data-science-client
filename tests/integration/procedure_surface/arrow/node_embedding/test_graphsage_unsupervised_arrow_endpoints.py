from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.node_embedding.graphsage_unsupervised_model import (
    GraphSageUnsupervisedModel,
)
from graphdatascience.procedure_surface.arrow.node_embedding.graphsage_unsupervised_arrow_endpoints import (
    GraphSageUnsupervisedArrowEndpoints,
)
from graphdatascience.query_runner import QueryRunner, QueryType
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol
from tests.integration.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

graph = """
    CREATE
    (a: Node {feature: 1.0}),
    (b: Node {feature: 2.0}),
    (c: Node {feature: 3.0}),
    (d: Node {feature: 4.0}),
    (a)-[:REL]->(b),
    (b)-[:REL]->(c),
    (c)-[:REL]->(d),
    (d)-[:REL]->(a)
    """


@pytest.fixture
def sample_graph(arrow_client_runtime: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    with create_graph(arrow_client_runtime, "gs-unsup-g", graph) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client_runtime: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[Graph, None, None]:
    with create_graph_from_db(
        arrow_client_runtime,
        query_runner,
        "gs-unsup-g",
        graph,
        """
            MATCH (n)-->(m)
            WITH gds.graph.project.remote(
                n,
                m,
                {sourceNodeProperties: properties(n), targetNodeProperties: properties(m)}
            ) as g
            RETURN g
        """,
    ) as g:
        yield g


@pytest.fixture
def gs_model(
    arrow_client_runtime: AuthenticatedArrowClient, sample_graph: Graph
) -> Generator[GraphSageUnsupervisedModel, None, None]:
    endpoints = GraphSageUnsupervisedArrowEndpoints(arrow_client_runtime, None, show_progress=False)
    model, _ = endpoints.train(
        G=sample_graph,
        model_name="gs-unsup-model",
        feature_properties=["feature"],
        embedding_dimension=1,
        epochs=1,
        num_walks=1,
        walk_depth=1,
    )

    yield model

    model.drop()


def test_train(gs_model: GraphSageUnsupervisedModel) -> None:
    assert gs_model.name() == "gs-unsup-model"
    assert gs_model.exists()


def test_stream(gs_model: GraphSageUnsupervisedModel, sample_graph: Graph) -> None:
    result = gs_model.predict_stream(sample_graph, feature_properties=["feature"])

    assert set(result.columns) == {"nodeId", "embedding"}
    assert len(result) == 4


def test_mutate(gs_model: GraphSageUnsupervisedModel, sample_graph: Graph) -> None:
    result = gs_model.predict_mutate(sample_graph, feature_properties=["feature"], mutate_property="embedding")

    assert result.node_properties_written == 4
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0


@pytest.mark.db_integration
def test_write(arrow_client_runtime: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: Graph) -> None:
    endpoints = GraphSageUnsupervisedArrowEndpoints(
        arrow_client_runtime, WriteProtocol.select(arrow_client_runtime, query_runner), show_progress=False
    )
    model, _ = endpoints.train(
        G=db_graph,
        model_name="gs-unsup-model-write",
        feature_properties=["feature"],
        embedding_dimension=1,
        epochs=1,
        num_walks=1,
        walk_depth=1,
    )

    try:
        result = model.predict_write(db_graph, feature_properties=["feature"], write_property="embedding")

        assert result.node_properties_written == 4
        assert result.compute_millis >= 0
        assert result.write_millis >= 0

        assert (
            query_runner.run_cypher(
                "MATCH (n) WHERE n.embedding IS NOT NULL RETURN COUNT(*) AS count", query_type=QueryType.USER_ACTION
            ).iloc[0, 0]
            == 4
        )
    finally:
        model.drop()
