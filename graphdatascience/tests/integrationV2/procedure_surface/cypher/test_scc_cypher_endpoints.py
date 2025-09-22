from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.scc_endpoints import (
    SccMutateResult,
    SccStatsResult,
    SccWriteResult,
)
from graphdatascience.procedure_surface.cypher.scc_cypher_endpoints import SccCypherEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
        CREATE
              (a:Node)
            , (b:Node)
            , (c:Node)
            , (d:Node)
            , (e:Node)
            , (f:Node)
            , (g:Node)
            , (h:Node)
            , (i:Node)
            , (a)-[:TYPE {cost: 5}]->(b)
            , (b)-[:TYPE {cost: 5}]->(c)
            , (c)-[:TYPE {cost: 5}]->(a)
            , (d)-[:TYPE {cost: 2}]->(e)
            , (e)-[:TYPE {cost: 2}]->(f)
            , (f)-[:TYPE {cost: 2}]->(d)
            , (a)-[:TYPE {cost: 2}]->(d)
            , (g)-[:TYPE {cost: 3}]->(h)
            , (h)-[:TYPE {cost: 3}]->(i)
            , (i)-[:TYPE {cost: 3}]->(g)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {}) AS G
        RETURN G
    """

    with create_graph(
        query_runner,
        "g",
        create_statement,
        projection_query,
    ) as g:
        yield g


@pytest.fixture
def scc_endpoints(query_runner: QueryRunner) -> SccCypherEndpoints:
    return SccCypherEndpoints(query_runner)


def test_scc_stats(scc_endpoints: SccCypherEndpoints, sample_graph: GraphV2) -> None:
    result = scc_endpoints.stats(sample_graph)

    assert isinstance(result, SccStatsResult)
    assert result.component_count == 3
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert "p10" in result.component_distribution


def test_scc_stream(scc_endpoints: SccCypherEndpoints, sample_graph: GraphV2) -> None:
    result = scc_endpoints.stream(sample_graph)

    assert len(result) == 9
    assert "nodeId" in result.columns
    assert "componentId" in result.columns


def test_scc_mutate(scc_endpoints: SccCypherEndpoints, sample_graph: GraphV2) -> None:
    result = scc_endpoints.mutate(sample_graph, "componentId")

    assert isinstance(result, SccMutateResult)
    assert result.component_count == 3
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 9
    assert "p10" in result.component_distribution


def test_scc_estimate(scc_endpoints: SccCypherEndpoints, sample_graph: GraphV2) -> None:
    result = scc_endpoints.estimate(G=sample_graph)

    assert result.node_count == 9
    assert result.relationship_count == 10
    assert result.required_memory is not None
    assert result.tree_view is not None
    assert isinstance(result.map_view, dict)
    assert result.bytes_min >= 0
    assert result.bytes_max >= 0
    assert result.heap_percentage_min >= 0
    assert result.heap_percentage_max >= 0


def test_scc_write(scc_endpoints: SccCypherEndpoints, sample_graph: GraphV2) -> None:
    result = scc_endpoints.write(sample_graph, "componentId")

    assert isinstance(result, SccWriteResult)
    assert result.component_count == 3
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.node_properties_written == 9
    assert "p10" in result.component_distribution
