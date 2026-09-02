import uuid
from typing import Generator

import pytest
from testcontainers.community.neo4j import Neo4jContainer

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.cypher.catalog.graph_export_cypher_endpoints import (
    GraphExportCypherEndpoints,
)
from graphdatascience.query_runner import QueryRunner
from tests.integration.procedure_surface.cypher.cypher_graph_helper import (
    create_graph,
)


def assert_files_written(container: Neo4jContainer, directory: str) -> None:
    exit_code, output = container.exec(["ls", "-A", directory])
    assert exit_code == 0, f"Expected directory '{directory}' to exist in the container"
    assert output.decode().strip(), f"Expected files to be written to '{directory}'"


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[Graph, None, None]:
    create_statement = """
    CREATE
    (a: Node {id: 0}),
    (b: Node {id: 1}),
    (c: Node {id: 2}),
    (d: Node {id: 3}),
    (e: Node {id: 4}),
    (a)-[:REL {weight: 1.0}]->(b),
    (b)-[:REL {weight: 2.0}]->(c),
    (c)-[:REL {weight: 1.5}]->(d),
    (d)-[:REL {weight: 0.5}]->(e),
    (e)-[:REL {weight: 1.2}]->(a)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {
            sourceNodeProperties: {id: n.id},
            targetNodeProperties: {id: m.id},
            relationshipProperties: {weight: r.weight}
        }) AS G
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
def graph_export_endpoints(query_runner: QueryRunner) -> Generator[GraphExportCypherEndpoints, None, None]:
    yield GraphExportCypherEndpoints(query_runner)


def test_graph_export(
    graph_export_endpoints: GraphExportCypherEndpoints, sample_graph: Graph, gds_plugin_container: Neo4jContainer
) -> None:
    db_name = f"exportdb{uuid.uuid4().hex[:8]}"

    result = graph_export_endpoints(G=sample_graph, db_name=db_name)

    assert result.db_name == db_name
    assert result.graph_name == sample_graph.name()
    assert result.node_count == 5
    assert result.node_property_count == 5
    assert result.relationship_count == 5
    assert result.relationship_property_count == 5
    assert result.relationship_type_count == 1
    assert result.write_millis >= 0

    assert_files_written(gds_plugin_container, f"/data/databases/{db_name}")


def test_graph_export_with_config(
    graph_export_endpoints: GraphExportCypherEndpoints, sample_graph: Graph, gds_plugin_container: Neo4jContainer
) -> None:
    db_name = f"exportdb{uuid.uuid4().hex[:8]}"

    result = graph_export_endpoints(
        G=sample_graph,
        db_name=db_name,
        batch_size=100,
        default_relationship_type="DEFAULT",
        enable_debug_log=True,
        write_concurrency=2,
    )

    assert result.db_name == db_name
    assert result.node_count == 5
    assert result.relationship_count == 5

    assert_files_written(gds_plugin_container, f"/data/databases/{db_name}")


def test_graph_export_csv(
    graph_export_endpoints: GraphExportCypherEndpoints, sample_graph: Graph, gds_plugin_container: Neo4jContainer
) -> None:
    export_name = f"csvexport{uuid.uuid4().hex[:8]}"

    result = graph_export_endpoints.csv(G=sample_graph, export_name=export_name)

    assert result.export_name == export_name
    assert result.graph_name == sample_graph.name()
    assert result.node_count == 5
    assert result.node_property_count == 5
    assert result.relationship_count == 5
    assert result.relationship_property_count == 5
    assert result.relationship_type_count == 1
    assert result.write_millis >= 0

    assert_files_written(gds_plugin_container, f"/exports/export/{export_name}")


def test_graph_export_csv_with_config(
    graph_export_endpoints: GraphExportCypherEndpoints, sample_graph: Graph, gds_plugin_container: Neo4jContainer
) -> None:
    export_name = f"csvexport{uuid.uuid4().hex[:8]}"

    result = graph_export_endpoints.csv(
        G=sample_graph,
        export_name=export_name,
        batch_size=100,
        include_meta_data=True,
        use_label_mapping=True,
        write_concurrency=2,
    )

    assert result.export_name == export_name
    assert result.node_count == 5
    assert result.relationship_count == 5

    assert_files_written(gds_plugin_container, f"/exports/export/{export_name}")
