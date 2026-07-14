import logging
from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.arrow.catalog import ProjectionResult
from graphdatascience.procedure_surface.arrow.catalog.catalog_arrow_endpoints import (
    CatalogArrowEndpoints,
)
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.query_type import QueryType
from tests.integration.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    gdl = """
    (a :Node:A)
    (b :Node:A)
    (c :Node:B)
    (a)-[:REL]->(c)
    """

    with create_graph(arrow_client, "g", gdl) as G:
        yield G


@pytest.fixture
def endpoints(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner
) -> Generator[CatalogArrowEndpoints, None, None]:
    yield CatalogArrowEndpoints(arrow_client, query_runner)


@pytest.mark.db_integration
def test_projection(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, endpoints: CatalogArrowEndpoints
) -> None:
    try:
        G, result = endpoints.project.cypher(
            graph_name="g",
            query="UNWIND range(1, 10) AS x WITH gds.graph.project.remote(x, null) as g RETURN g",
        )

        assert isinstance(result, ProjectionResult)

        assert G.name() == "g"
        assert result.graph_name == "g"
        assert result.node_count == 10
        assert result.relationship_count == 0
        assert result.project_millis >= 0

        assert len(endpoints.list("g")) == 1
    finally:
        endpoints.drop("g", fail_if_missing=False)


@pytest.mark.db_integration
def test_projection_with_query_parameters(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, endpoints: CatalogArrowEndpoints
) -> None:
    try:
        G, result = endpoints.project.cypher(
            graph_name="g",
            query="UNWIND range(1, $LIMIT) AS x WITH gds.graph.project.remote(x, null, {undirectedRelationshipTypes:['foo']}) as g RETURN g",
            query_parameters={"LIMIT": 10},
        )

        assert isinstance(result, ProjectionResult)

        assert G.name() == "g"
        assert result.graph_name == "g"
        assert result.node_count == 10
        assert result.relationship_count == 0
        assert result.project_millis >= 0

        assert len(endpoints.list("g")) == 1
    finally:
        endpoints.drop("g", fail_if_missing=False)


@pytest.mark.db_integration
def test_projection_with_query_rewrite(
    arrow_client: AuthenticatedArrowClient,
    query_runner: QueryRunner,
    endpoints: CatalogArrowEndpoints,
    caplog: pytest.LogCaptureFixture,
) -> None:
    try:
        with caplog.at_level(logging.WARNING, logger="gds_arrow_client"):
            G, result = endpoints.project.cypher(
                graph_name="g",
                query="UNWIND range(1, 10) AS x WITH gds.graph.project(x, null) as g RETURN g",
            )

        assert any(
            "Remote cypher projections need to call `gds.graph.project.remote` instead of `gds.graph.project`."
            in record.message
            for record in caplog.records
        )

        assert isinstance(result, ProjectionResult)

        assert G.name() == "g"
        assert result.graph_name == "g"
        assert result.node_count == 10
        assert result.relationship_count == 0
        assert result.project_millis >= 0

        assert len(endpoints.list("g")) == 1
    finally:
        endpoints.drop("g", fail_if_missing=False)


@pytest.mark.db_integration
def test_projection_undirected_relationship_types_in_query(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, endpoints: CatalogArrowEndpoints
) -> None:
    with pytest.raises(ValueError, match="undirectedRelationshipTypes.*need to be specified as separate arguments"):
        endpoints.project.cypher(
            graph_name="g",
            query="UNWIND range(1, 10) AS x WITH gds.graph.project(x, null, {undirectedRelationshipTypes: ['TYPE']}) as g RETURN g",
        )


@pytest.mark.db_integration
def test_projection_inversed_indexed_relationship_types_in_query(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, endpoints: CatalogArrowEndpoints
) -> None:
    with pytest.raises(
        ValueError, match=".*inverseIndexedRelationshipTypes.*need to be specified as separate arguments"
    ):
        endpoints.project.cypher(
            graph_name="g",
            query="UNWIND range(1, 10) AS x WITH gds.graph.project(x, null, {inverseIndexedRelationshipTypes: ['TYPE']}) as g RETURN g",
        )


@pytest.mark.db_integration
def test_store_projection(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, endpoints: CatalogArrowEndpoints
) -> None:
    try:
        query_runner.run_cypher(
            "UNWIND range(1, 5) AS x CREATE (:Person)-[:KNOWS]->(:Person)",
            QueryType.USER_ACTION,
        )

        G, result = endpoints.project.native(
            graph_name="g",
            node_label_filter=["Person"],
            relationship_type_filter=["KNOWS"],
        )

        assert G.name() == "g"
        assert result.graph_name == "g"
        assert result.node_count == 10
        assert result.relationship_count == 5

        assert len(endpoints.list("g")) == 1
    finally:
        endpoints.drop("g", fail_if_missing=False)
        query_runner.run_cypher("MATCH (n) DETACH DELETE n", QueryType.USER_ACTION)


@pytest.mark.db_integration
def test_projection_async(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, endpoints: CatalogArrowEndpoints
) -> None:
    try:
        handle = endpoints.project.cypher_async(
            graph_name="g",
            query="UNWIND range(1, 10) AS x WITH gds.graph.project.remote(x, null) as g RETURN g",
        )

        assert handle.job_id()

        handle.wait()
        assert handle.done()

        G, result = handle.result()

        assert G.name() == "g"
        assert result["graphName"] == "g"
        assert result["nodeCount"] == 10
        assert result["relationshipCount"] == 0
        assert result["projectMillis"] >= 0

        assert len(endpoints.list("g")) == 1
    finally:
        endpoints.drop("g", fail_if_missing=False)


@pytest.mark.db_integration
def test_store_projection_async(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, endpoints: CatalogArrowEndpoints
) -> None:
    try:
        query_runner.run_cypher(
            "UNWIND range(1, 5) AS x CREATE (:Person)-[:KNOWS]->(:Person)",
            QueryType.USER_ACTION,
        )

        handle = endpoints.project.native_async(
            graph_name="g",
            node_label_filter=["Person"],
            relationship_type_filter=["KNOWS"],
        )

        assert handle.job_id()

        G, result = handle.result()

        assert G.name() == "g"
        assert result["graphName"] == "g"
        assert result["nodeCount"] == 10
        assert result["relationshipCount"] == 5

        assert len(endpoints.list("g")) == 1
    finally:
        endpoints.drop("g", fail_if_missing=False)
        query_runner.run_cypher("MATCH (n) DETACH DELETE n", QueryType.USER_ACTION)
