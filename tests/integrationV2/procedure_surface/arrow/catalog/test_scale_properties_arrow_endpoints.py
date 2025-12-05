from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.scale_properties_endpoints import ScalePropertiesWriteResult
from graphdatascience.procedure_surface.api.catalog.scaler_config import ScalerConfig
from graphdatascience.procedure_surface.arrow.catalog.scale_properties_arrow_endpoints import (
    ScalePropertiesArrowEndpoints,
)
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

graph = """
        CREATE
            (a: Node {id: 0, prop1: 1.0, prop2: 5.0}),
            (b: Node {id: 1, prop1: 2.0, prop2: 10.0}),
            (c: Node {id: 2, prop1: 3.0, prop2: 15.0}),
            (a)-[:REL]->(c),
            (b)-[:REL]->(c)
        """


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(arrow_client, "g", graph) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    with create_graph_from_db(
        arrow_client,
        query_runner,
        "g",
        graph,
        """
                    MATCH (n)-->(m)
                    WITH gds.graph.project.remote(n, m, {sourceNodeProperties: properties(n), targetNodeProperties: properties(m)}) as g
                    RETURN g
                """,
    ) as g:
        yield g


@pytest.fixture
def scale_properties_endpoints(
    arrow_client: AuthenticatedArrowClient,
) -> Generator[ScalePropertiesArrowEndpoints, None, None]:
    yield ScalePropertiesArrowEndpoints(arrow_client)


def test_scale_properties_stats(
    scale_properties_endpoints: ScalePropertiesArrowEndpoints, sample_graph: GraphV2
) -> None:
    result = scale_properties_endpoints.stats(
        G=sample_graph, node_properties=["prop1"], scaler=ScalerConfig(type="MinMax")
    )

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert "prop1" in result.scaler_statistics
    assert isinstance(result.configuration, dict)


def test_scale_properties_stream(
    scale_properties_endpoints: ScalePropertiesArrowEndpoints, sample_graph: GraphV2
) -> None:
    result_df = scale_properties_endpoints.stream(
        G=sample_graph, node_properties=["prop1"], scaler=ScalerConfig(type="Log", offset=1.0)
    )

    assert set(result_df.columns) == {"nodeId", "scaledProperty"}
    assert len(result_df) == 3  # We have 3 nodes


def test_scale_properties_mutate(
    scale_properties_endpoints: ScalePropertiesArrowEndpoints, sample_graph: GraphV2
) -> None:
    result = scale_properties_endpoints.mutate(
        G=sample_graph, mutate_property="scaledProp", node_properties=["prop1"], scaler="MinMax"
    )

    assert result.node_properties_written == 3
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert "prop1" in result.scaler_statistics
    assert isinstance(result.configuration, dict)


@pytest.mark.db_integration
def test_scale_properties_write(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: GraphV2
) -> None:
    endpoints = ScalePropertiesArrowEndpoints(arrow_client, RemoteWriteBackClient(arrow_client, query_runner))
    result = endpoints.write(
        G=db_graph, write_property="scaledProp", node_properties=["prop1"], scaler={"type": "MinMax"}
    )

    assert isinstance(result, ScalePropertiesWriteResult)
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 3
    assert "prop1" in result.scaler_statistics

    assert query_runner.run_cypher("MATCH (n) WHERE n.scaledProp IS NOT NULL RETURN COUNT(*) AS count").squeeze() == 3


def test_scale_properties_estimate(
    scale_properties_endpoints: ScalePropertiesArrowEndpoints, sample_graph: GraphV2
) -> None:
    result = scale_properties_endpoints.estimate(sample_graph, node_properties=["prop1"], scaler="MinMax")

    assert result.node_count == 3
    assert result.relationship_count == 2
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
