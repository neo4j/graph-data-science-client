from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.collapse_path_cypher_endpoints import CollapsePathCypherEndpoints
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
def collapse_path_endpoints(
    query_runner: Neo4jQueryRunner,
) -> Generator[CollapsePathCypherEndpoints, None, None]:
    yield CollapsePathCypherEndpoints(query_runner)


def test_collapse_path(collapse_path_endpoints: CollapsePathCypherEndpoints, sample_graph: GraphV2) -> None:
    result = collapse_path_endpoints.mutate(
        G=sample_graph,
        path_templates=[["REL", "REL"]],
        mutate_relationship_type="FoF",
    )

    assert result.relationshipsWritten == 3
    assert result.mutateMillis >= 0
    assert result.preProcessingMillis >= 0
    assert result.computeMillis >= 0
