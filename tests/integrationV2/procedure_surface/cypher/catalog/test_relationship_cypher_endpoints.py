from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.v1.gds_arrow_client import GdsArrowClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.relationships_endpoints import Aggregation
from graphdatascience.procedure_surface.cypher.catalog.relationship_cypher_endpoints import (
    RelationshipCypherEndpoints,
)
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_query = """
        CREATE
        (a:Node),
        (b:Node),
        (c:Node),
        (a)-[:REL {weight: 1.0, weight2: 42.0}]->(b),
        (b)-[:REL {weight: 2.0, weight2: 43.0}]->(c),
        (c)-[:REL {weight: 3.0, weight2: 44.0}]->(a),
        (a)-[:OTHER {value: 10}]->(c)
    """

    projection_query = """
        MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {
            relationshipType: type(r),
            relationshipProperties: properties(r)
        }) AS G
        RETURN G
    """

    with create_graph(
        query_runner,
        "g",
        create_query,
        projection_query,
    ) as g:
        yield g


@pytest.fixture
def relationship_endpoints(
    query_runner: Neo4jQueryRunner,
) -> Generator[RelationshipCypherEndpoints, None, None]:
    yield RelationshipCypherEndpoints(query_runner)


def test_stream_relationships(relationship_endpoints: RelationshipCypherEndpoints, sample_graph: GraphV2) -> None:
    result = relationship_endpoints.stream(G=sample_graph, relationship_types=["REL"])

    assert len(result) == 3
    assert "sourceNodeId" in result.columns
    assert "targetNodeId" in result.columns
    assert "relationshipType" in result.columns
    assert set(result["relationshipType"].unique()) == {"REL"}


def test_stream_multiple_relationship_types(
    relationship_endpoints: RelationshipCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = relationship_endpoints.stream(G=sample_graph, relationship_types=["REL", "OTHER"])

    assert len(result) == 4  # 3 REL + 1 OTHER
    assert "sourceNodeId" in result.columns
    assert "targetNodeId" in result.columns
    assert "relationshipType" in result.columns
    assert set(result["relationshipType"].unique()) == {"REL", "OTHER"}


def test_stream_all_relationships(relationship_endpoints: RelationshipCypherEndpoints, sample_graph: GraphV2) -> None:
    result = relationship_endpoints.stream(G=sample_graph)

    assert len(result) == 4  # All relationships
    assert "sourceNodeId" in result.columns
    assert "targetNodeId" in result.columns
    assert "relationshipType" in result.columns
    assert set(result["relationshipType"].unique()) == {"REL", "OTHER"}


def test_stream_single_property(relationship_endpoints: RelationshipCypherEndpoints, sample_graph: GraphV2) -> None:
    result = relationship_endpoints.stream(
        G=sample_graph, relationship_types=["REL"], relationship_properties=["weight"]
    )

    assert len(result) == 3  # All relationships
    assert "sourceNodeId" in result.columns
    assert "targetNodeId" in result.columns
    assert "relationshipType" in result.columns
    assert "weight" in result.columns
    assert set(result["relationshipType"].unique()) == {"REL"}
    assert set(result["weight"].unique()) == {1.0, 2.0, 3.0}


def test_stream_multiple_properties(relationship_endpoints: RelationshipCypherEndpoints, sample_graph: GraphV2) -> None:
    result = relationship_endpoints.stream(
        G=sample_graph, relationship_types=["REL"], relationship_properties=["weight", "weight2"]
    )

    assert len(result) == 6  # All relationships
    assert "sourceNodeId" in result.columns
    assert "targetNodeId" in result.columns
    assert "relationshipType" in result.columns
    assert "propertyValue" in result.columns
    assert "relationshipProperty" in result.columns
    assert set(result["relationshipProperty"].unique()) == {"weight", "weight2"}


def test_stream_relationships_with_arrow(
    query_runner: QueryRunner, gds_arrow_client: GdsArrowClient, sample_graph: GraphV2
) -> None:
    endpoints = RelationshipCypherEndpoints(query_runner, gds_arrow_client)

    result = endpoints.stream(G=sample_graph, relationship_types=["REL"])

    assert len(result) == 3
    assert "sourceNodeId" in result.columns
    assert "targetNodeId" in result.columns
    assert "relationshipType" in result.columns
    assert set(result["relationshipType"].unique()) == {"REL"}


def test_stream_relationship_properties_with_arrow(
    query_runner: QueryRunner, gds_arrow_client: GdsArrowClient, sample_graph: GraphV2
) -> None:
    endpoints = RelationshipCypherEndpoints(query_runner, gds_arrow_client)

    result = endpoints.stream(G=sample_graph, relationship_types=["REL"], relationship_properties=["weight"])

    assert len(result) == 3
    assert "sourceNodeId" in result.columns
    assert "targetNodeId" in result.columns
    assert "relationshipType" in result.columns
    assert "weight" in result.columns
    assert set(result["relationshipType"].unique()) == {"REL"}
    assert set(result["weight"].unique()) == {1.0, 2.0, 3.0}


@pytest.mark.db_integration
def test_write_relationships(
    relationship_endpoints: RelationshipCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner
) -> None:
    result = relationship_endpoints.write(G=sample_graph, relationship_type="REL")

    assert result.graph_name == sample_graph.name()
    assert result.relationship_type == "REL"
    assert result.relationship_properties == []
    assert result.relationships_written == 3
    assert result.properties_written == 0
    assert result.write_millis >= 0


@pytest.mark.db_integration
def test_write_relationships_with_single_property(
    relationship_endpoints: RelationshipCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner
) -> None:
    result = relationship_endpoints.write(G=sample_graph, relationship_type="REL", relationship_properties=["weight"])

    assert result.graph_name == sample_graph.name()
    assert result.relationship_type == "REL"
    assert result.relationship_properties == ["weight"]
    assert result.relationships_written == 3
    assert result.properties_written == 3
    assert result.write_millis >= 0


@pytest.mark.db_integration
def test_write_relationships_with_multiple_properties(
    relationship_endpoints: RelationshipCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner
) -> None:
    result = relationship_endpoints.write(
        G=sample_graph, relationship_type="REL", relationship_properties=["weight", "weight2"]
    )

    assert result.graph_name == sample_graph.name()
    assert result.relationship_type == "REL"
    assert result.relationship_properties == ["weight", "weight2"]
    assert result.relationships_written == 3
    assert result.properties_written == 6
    assert result.write_millis >= 0


def test_drop_relationships(relationship_endpoints: RelationshipCypherEndpoints, sample_graph: GraphV2) -> None:
    # Drop REL relationship type
    drop_result = relationship_endpoints.drop(G=sample_graph, relationship_type="REL")

    assert drop_result.graph_name == sample_graph.name()
    assert drop_result.relationship_type == "REL"
    assert drop_result.deleted_relationships == 3
    assert drop_result.deleted_properties == {"weight": 3, "weight2": 3}


def test_index_inverse(relationship_endpoints: RelationshipCypherEndpoints, sample_graph: GraphV2) -> None:
    result = relationship_endpoints.index_inverse(G=sample_graph, relationship_types=["REL"])

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.input_relationships == 3
    assert "configuration" in result.__dict__


def test_to_undirected(relationship_endpoints: RelationshipCypherEndpoints, sample_graph: GraphV2) -> None:
    result = relationship_endpoints.to_undirected(
        G=sample_graph, relationship_type="REL", mutate_relationship_type="REL_UNDIRECTED"
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.input_relationships == 3
    assert result.relationships_written == 6


def test_to_undirected_with_aggregation(
    relationship_endpoints: RelationshipCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = relationship_endpoints.to_undirected(
        G=sample_graph,
        relationship_type="REL",
        mutate_relationship_type="REL_UNDIRECTED_SUM",
        aggregation=Aggregation.SUM,
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.input_relationships == 3
    assert result.relationships_written == 6


def test_to_undirected_with_property_aggregation(
    relationship_endpoints: RelationshipCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = relationship_endpoints.to_undirected(
        G=sample_graph,
        relationship_type="REL",
        mutate_relationship_type="REL_UNDIRECTED_DICT",
        aggregation={"weight": Aggregation.MAX, "weight2": Aggregation.MIN},
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.input_relationships == 3
    assert result.relationships_written == 6


def test_collapse_path(relationship_endpoints: RelationshipCypherEndpoints, sample_graph: GraphV2) -> None:
    result = relationship_endpoints.collapse_path(
        G=sample_graph,
        path_templates=[["REL", "REL"]],
        mutate_relationship_type="FoF",
    )

    assert result.relationshipsWritten == 3
    assert result.mutateMillis >= 0
    assert result.preProcessingMillis >= 0
    assert result.computeMillis >= 0
