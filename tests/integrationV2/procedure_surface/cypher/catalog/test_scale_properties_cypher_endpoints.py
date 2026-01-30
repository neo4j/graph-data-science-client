from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.scale_properties_endpoints import ScalePropertiesWriteResult
from graphdatascience.procedure_surface.api.catalog.scaler_config import ScalerConfig
from graphdatascience.procedure_surface.cypher.catalog.scale_properties_cypher_endpoints import (
    ScalePropertiesCypherEndpoints,
)
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph

graph = """
        CREATE
            (a: Node {id: 0, prop1: 1.0, prop2: 5.0}),
            (b: Node {id: 1, prop1: 2.0, prop2: 10.0}),
            (c: Node {id: 2, prop1: 3.0, prop2: 15.0}),
            (a)-[:REL]->(c),
            (b)-[:REL]->(c)
        """

projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {sourceNodeProperties: properties(n), targetNodeProperties: properties(m)}) AS G
        RETURN G
    """


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    with create_graph(query_runner, "g", graph, projection_query) as G:
        yield G


@pytest.fixture
def scale_properties_endpoints(query_runner: QueryRunner) -> Generator[ScalePropertiesCypherEndpoints, None, None]:
    yield ScalePropertiesCypherEndpoints(query_runner)


def test_scale_properties_stats(
    scale_properties_endpoints: ScalePropertiesCypherEndpoints, sample_graph: GraphV2
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
    scale_properties_endpoints: ScalePropertiesCypherEndpoints, sample_graph: GraphV2
) -> None:
    result_df = scale_properties_endpoints.stream(
        G=sample_graph, node_properties=["prop1"], scaler=ScalerConfig(type="Log", offset=1.0)
    )

    assert set(result_df.columns) == {"nodeId", "scaledProperty"}
    assert len(result_df) == 3  # We have 3 nodes


def test_scale_properties_mutate(
    scale_properties_endpoints: ScalePropertiesCypherEndpoints, sample_graph: GraphV2
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


def test_scale_properties_write(
    scale_properties_endpoints: ScalePropertiesCypherEndpoints, query_runner: QueryRunner, sample_graph: GraphV2
) -> None:
    result = scale_properties_endpoints.write(
        G=sample_graph, write_property="scaledProp", node_properties=["prop1"], scaler="MinMax"
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
    scale_properties_endpoints: ScalePropertiesCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = scale_properties_endpoints.estimate(sample_graph, node_properties=["prop1"], scaler="MinMax")

    assert result.node_count == 3
    assert result.relationship_count == 2
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
