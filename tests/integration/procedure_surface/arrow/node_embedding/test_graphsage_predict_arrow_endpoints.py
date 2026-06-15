import json
from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.model.graphsage_model import GraphSageModelV2
from graphdatascience.procedure_surface.arrow.node_embedding.graphsage_predict_arrow_endpoints import (
    GraphSagePredictArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.node_embedding.graphsage_train_arrow_endpoints import (
    GraphSageTrainArrowEndpoints,
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
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    with create_graph(arrow_client, "g", graph) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[Graph, None, None]:
    with create_graph_from_db(
        arrow_client,
        query_runner,
        "g",
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
def gs_model(arrow_client: AuthenticatedArrowClient, sample_graph: Graph) -> Generator[GraphSageModelV2, None, None]:
    model, _ = GraphSageTrainArrowEndpoints(arrow_client, None)(
        G=sample_graph,
        model_name="gs-model",
        feature_properties=["feature"],
        embedding_dimension=1,
        sample_sizes=[1],
        max_iterations=1,
        search_depth=1,
    )

    yield model

    arrow_client.do_action_with_retry("v2/model.drop", json.dumps({"modelName": model.name()}).encode("utf-8"))


def test_stream(gs_model: GraphSageModelV2, sample_graph: Graph) -> None:
    result = gs_model.predict_stream(sample_graph, concurrency=4)

    assert set(result.columns) == {"nodeId", "embedding"}
    assert len(result) == 4


def test_mutate(gs_model: GraphSageModelV2, sample_graph: Graph) -> None:
    result = gs_model.predict_mutate(sample_graph, concurrency=4, mutate_property="embedding")

    assert result.node_properties_written == 4


@pytest.mark.db_integration
def test_write(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: Graph) -> None:
    model, _ = GraphSageTrainArrowEndpoints(arrow_client, WriteProtocol.select(arrow_client, query_runner))(
        G=db_graph,
        model_name="gs-model-write",
        feature_properties=["feature"],
        embedding_dimension=1,
        sample_sizes=[1],
        max_iterations=1,
        search_depth=1,
    )

    try:
        result = model.predict_write(db_graph, write_property="embedding", concurrency=4, write_concurrency=2)

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
        arrow_client.do_action_with_retry("v2/model.drop", json.dumps({"modelName": model.name()}).encode("utf-8"))


def test_write_without_write_back_client(gs_model: GraphSageModelV2, sample_graph: Graph) -> None:
    with pytest.raises(Exception, match="Write back is not supported by this session."):
        gs_model.predict_write(sample_graph, write_property="embedding", concurrency=4, write_concurrency=2)


def test_estimate(gs_model: GraphSageModelV2, sample_graph: Graph) -> None:
    result = gs_model.predict_estimate(sample_graph, concurrency=4)

    assert result.node_count == 4
    assert result.relationship_count == 4
    assert "KiB" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0


def test_compute(arrow_client: AuthenticatedArrowClient, gs_model: GraphSageModelV2, sample_graph: Graph) -> None:
    endpoints = GraphSagePredictArrowEndpoints(arrow_client, None, show_progress=False)
    handle = endpoints.compute(G=sample_graph, model_name=gs_model.name(), concurrency=4)
    summary = handle.summary()

    assert summary["computeMillis"] >= 0
    assert "writeProperty" not in summary["configuration"]

    df = handle.stream()
    assert set(df.columns) == {"nodeId", "embedding"}
    assert len(df) == 4
