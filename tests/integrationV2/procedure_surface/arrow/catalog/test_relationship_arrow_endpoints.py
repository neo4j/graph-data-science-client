from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.relationships_endpoints import Aggregation
from graphdatascience.procedure_surface.arrow.catalog.relationship_arrow_endpoints import (
    RelationshipArrowEndpoints,
)
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    gdl = """
        CREATE
        (a:Node),
        (b:Node),
        (c:Node),
        (a)-[:REL {weight: 1.0}]->(b),
        (b)-[:REL {weight: 2.0}]->(c),
        (c)-[:REL {weight: 3.0}]->(a),
        (a)-[:OTHER {value: 10}]->(c)
    """

    with create_graph(arrow_client, "g", gdl) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    graph_data = """
        CREATE
        (a: Node),
        (b: Node),
        (c: Node),
        (a)-[:REL {weight: 1.0}]->(b),
        (b)-[:REL {weight: 2.0}]->(c),
        (c)-[:REL {weight: 3.0}]->(a)
    """

    projection_query = """
    MATCH (n)-[r]->(m)
    WITH gds.graph.project.remote(
        n,
        m,
        {
            relationshipType: type(r),
            relationshipProperties: {weight: r.weight}
        }
    ) as g
    RETURN g
    """

    with create_graph_from_db(
        arrow_client,
        query_runner,
        "g",
        graph_data,
        projection_query,
    ) as G:
        yield G


@pytest.fixture
def relationship_endpoints(
    arrow_client: AuthenticatedArrowClient,
) -> Generator[RelationshipArrowEndpoints, None, None]:
    yield RelationshipArrowEndpoints(arrow_client, None)


@pytest.fixture
def relationship_endpoints_with_db(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner
) -> Generator[RelationshipArrowEndpoints, None, None]:
    yield RelationshipArrowEndpoints(arrow_client, RemoteWriteBackClient(arrow_client, query_runner))


def test_stream_relationships(relationship_endpoints: RelationshipArrowEndpoints, sample_graph: GraphV2) -> None:
    result = relationship_endpoints.stream(G=sample_graph, relationship_types=["REL"])

    assert len(result) == 3
    assert "sourceNodeId" in result.columns
    assert "targetNodeId" in result.columns
    assert "relationshipType" in result.columns
    assert set(result["relationshipType"].unique()) == {"REL"}


def test_stream_multiple_relationship_types(
    relationship_endpoints: RelationshipArrowEndpoints, sample_graph: GraphV2
) -> None:
    result = relationship_endpoints.stream(G=sample_graph, relationship_types=["REL", "OTHER"])

    assert len(result) == 4  # 3 REL + 1 OTHER
    assert "sourceNodeId" in result.columns
    assert "targetNodeId" in result.columns
    assert "relationshipType" in result.columns
    assert set(result["relationshipType"].unique()) == {"REL", "OTHER"}


def test_stream_all_relationships(relationship_endpoints: RelationshipArrowEndpoints, sample_graph: GraphV2) -> None:
    result = relationship_endpoints.stream(G=sample_graph)

    assert len(result) == 4  # All relationships
    assert "sourceNodeId" in result.columns
    assert "targetNodeId" in result.columns
    assert "relationshipType" in result.columns
    assert set(result["relationshipType"].unique()) == {"REL", "OTHER"}


def test_stream_with_properties(relationship_endpoints: RelationshipArrowEndpoints, sample_graph: GraphV2) -> None:
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


@pytest.mark.db_integration
def test_write_relationships(
    relationship_endpoints_with_db: RelationshipArrowEndpoints, db_graph: GraphV2, query_runner: QueryRunner
) -> None:
    result = relationship_endpoints_with_db.write(G=db_graph, relationship_type="REL")

    assert result.graph_name == db_graph.name()
    assert result.relationship_type == "REL"
    assert result.write_millis >= 0
    assert result.relationships_written == 3


@pytest.mark.db_integration
def test_write_relationships_with_properties(
    relationship_endpoints_with_db: RelationshipArrowEndpoints, db_graph: GraphV2, query_runner: QueryRunner
) -> None:
    result = relationship_endpoints_with_db.write(
        G=db_graph, relationship_type="REL", relationship_properties=["weight"]
    )

    assert result.graph_name == db_graph.name()
    assert result.relationship_type == "REL"
    assert result.relationship_properties == ["weight"]
    assert result.write_millis >= 0
    assert result.relationships_written == 3
    assert result.properties_written == 3

    props_written = query_runner.run_cypher("""
        MATCH (n)-[r]->(m)
        WHERE type(r) = "REL" AND r.weight IS NOT NULL
        RETURN COUNT(r) as written
        """).squeeze()

    assert props_written == 6  # Old and new relationships


def test_drop_relationships(relationship_endpoints: RelationshipArrowEndpoints, sample_graph: GraphV2) -> None:
    drop_result = relationship_endpoints.drop(G=sample_graph, relationship_type="REL")

    assert drop_result.graph_name == sample_graph.name()
    assert drop_result.relationship_type == "REL"
    assert drop_result.deleted_relationships == 3
    assert drop_result.deleted_properties == {"weight": 3}


def test_index_inverse(relationship_endpoints: RelationshipArrowEndpoints, sample_graph: GraphV2) -> None:
    result = relationship_endpoints.index_inverse(G=sample_graph, relationship_types=["REL"])

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.input_relationships == 3
    assert "configuration" in result.__dict__


def test_to_undirected(relationship_endpoints: RelationshipArrowEndpoints, sample_graph: GraphV2) -> None:
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
    relationship_endpoints: RelationshipArrowEndpoints, sample_graph: GraphV2
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
    relationship_endpoints: RelationshipArrowEndpoints, sample_graph: GraphV2
) -> None:
    result = relationship_endpoints.to_undirected(
        G=sample_graph,
        relationship_type="REL",
        mutate_relationship_type="REL_UNDIRECTED_DICT",
        aggregation={"weight": Aggregation.MAX},
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.input_relationships == 3
    assert result.relationships_written == 6


def test_collapse_path(relationship_endpoints: RelationshipArrowEndpoints, sample_graph: GraphV2) -> None:
    result = relationship_endpoints.collapse_path(
        G=sample_graph,
        path_templates=[["REL", "REL"]],
        mutate_relationship_type="FoF",
    )

    assert result.relationshipsWritten == 3
    assert result.mutateMillis >= 0
    assert result.preProcessingMillis >= 0
    assert result.computeMillis >= 0
