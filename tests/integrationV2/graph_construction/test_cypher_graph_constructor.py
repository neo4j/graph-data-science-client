from typing import Generator

import pytest
from pandas import DataFrame

from graphdatascience.graph_construction.cypher_graph_constructor import CypherGraphConstructor
from graphdatascience.procedure_surface.cypher.catalog.catalog_cypher_endpoints import CatalogCypherEndpoints
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


@pytest.fixture
def catalog(query_runner: Neo4jQueryRunner) -> Generator[CatalogCypherEndpoints, None, None]:
    yield CatalogCypherEndpoints(query_runner)


def _constructor(query_runner: Neo4jQueryRunner, graph_name: str, **kwargs: object) -> CypherGraphConstructor:
    return CypherGraphConstructor(query_runner=query_runner, graph_name=graph_name, **kwargs)  # type: ignore[arg-type]


@pytest.mark.filterwarnings("ignore: .*use Apache Arrow.*")
def test_nodes_only(query_runner: Neo4jQueryRunner, catalog: CatalogCypherEndpoints) -> None:
    graph_name = "cypher_nodes_only"
    nodes = DataFrame({"nodeId": [0, 1, 2]})

    try:
        _constructor(query_runner, graph_name).run([nodes], [])

        info = catalog.list(graph_name)
        assert len(info) == 1
        assert info[0].node_count == 3
        assert info[0].relationship_count == 0
    finally:
        catalog.drop(graph_name, fail_if_missing=False)


@pytest.mark.filterwarnings("ignore: .*use Apache Arrow.*")
def test_nodes_with_labels_and_properties(query_runner: Neo4jQueryRunner, catalog: CatalogCypherEndpoints) -> None:
    graph_name = "cypher_nodes_labels_props"
    nodes = DataFrame(
        {
            "nodeId": [0, 1, 2],
            "labels": [["A"], ["B"], ["A"]],
            "score": [1.0, 2.0, 3.0],
        }
    )

    try:
        _constructor(query_runner, graph_name).run([nodes], [])

        info = catalog.list(graph_name)
        assert len(info) == 1
        assert info[0].node_count == 3
        schema = info[0].graph_schema
        assert "A" in schema["nodes"]
        assert "B" in schema["nodes"]
    finally:
        catalog.drop(graph_name, fail_if_missing=False)


@pytest.mark.filterwarnings("ignore: .*use Apache Arrow.*")
def test_nodes_and_relationships(query_runner: Neo4jQueryRunner, catalog: CatalogCypherEndpoints) -> None:
    graph_name = "cypher_nodes_and_rels"
    nodes = DataFrame(
        {
            "nodeId": [0, 1, 2],
            "labels": [["Person"], ["Person"], ["Person"]],
        }
    )
    relationships = DataFrame(
        {
            "sourceNodeId": [0, 1],
            "targetNodeId": [1, 2],
            "relationshipType": ["KNOWS", "KNOWS"],
            "weight": [0.5, 1.5],
        }
    )

    try:
        _constructor(query_runner, graph_name).run([nodes], [relationships])

        info = catalog.list(graph_name)
        assert len(info) == 1
        assert info[0].node_count == 3
        assert info[0].relationship_count == 2
        assert "KNOWS" in info[0].graph_schema["relationships"]
    finally:
        catalog.drop(graph_name, fail_if_missing=False)


@pytest.mark.filterwarnings("ignore: .*use Apache Arrow.*")
def test_multiple_node_dfs(query_runner: Neo4jQueryRunner, catalog: CatalogCypherEndpoints) -> None:
    graph_name = "cypher_multi_node_dfs"
    nodes_a = DataFrame({"nodeId": [0, 1], "labels": [["A"], ["A"]]})
    nodes_b = DataFrame({"nodeId": [2, 3], "labels": [["B"], ["B"]]})

    try:
        _constructor(query_runner, graph_name).run([nodes_a, nodes_b], [])

        info = catalog.list(graph_name)
        assert len(info) == 1
        assert info[0].node_count == 4
        schema = info[0].graph_schema["nodes"]
        assert "A" in schema
        assert "B" in schema
    finally:
        catalog.drop(graph_name, fail_if_missing=False)


@pytest.mark.filterwarnings("ignore: .*use Apache Arrow.*")
def test_multiple_relationship_dfs(query_runner: Neo4jQueryRunner, catalog: CatalogCypherEndpoints) -> None:
    graph_name = "cypher_multi_rel_dfs"
    nodes = DataFrame({"nodeId": [0, 1, 2]})
    rels_a = DataFrame(
        {
            "sourceNodeId": [0],
            "targetNodeId": [1],
            "relationshipType": ["REL_A"],
        }
    )
    rels_b = DataFrame(
        {
            "sourceNodeId": [1],
            "targetNodeId": [2],
            "relationshipType": ["REL_B"],
        }
    )

    try:
        _constructor(query_runner, graph_name).run([nodes], [rels_a, rels_b])

        info = catalog.list(graph_name)
        assert len(info) == 1
        assert info[0].node_count == 3
        assert info[0].relationship_count == 2
        rel_types = info[0].graph_schema["relationships"]
        assert "REL_A" in rel_types
        assert "REL_B" in rel_types
    finally:
        catalog.drop(graph_name, fail_if_missing=False)


@pytest.mark.filterwarnings("ignore: .*use Apache Arrow.*")
def test_undirected_relationship_types(query_runner: Neo4jQueryRunner, catalog: CatalogCypherEndpoints) -> None:
    graph_name = "cypher_undirected"
    nodes = DataFrame({"nodeId": [0, 1, 2]})
    relationships = DataFrame(
        {
            "sourceNodeId": [0, 1],
            "targetNodeId": [1, 2],
            "relationshipType": ["REL", "REL"],
        }
    )

    try:
        _constructor(query_runner, graph_name, undirected_relationship_types=["REL"]).run([nodes], [relationships])

        info = catalog.list(graph_name)
        assert len(info) == 1
        # undirected means each relationship is stored in both directions
        assert info[0].relationship_count == 4
    finally:
        catalog.drop(graph_name, fail_if_missing=False)


@pytest.mark.filterwarnings("ignore: .*use Apache Arrow.*")
def test_overlapping_column_names_raises(query_runner: Neo4jQueryRunner) -> None:
    graph_name = "cypher_overlapping_cols"
    nodes = DataFrame({"nodeId": [0, 1], "weight": [1.0, 2.0]})
    relationships = DataFrame({"sourceNodeId": [0], "targetNodeId": [1], "weight": [0.5]})

    with pytest.raises(ValueError, match="disjoint column names"):
        _constructor(query_runner, graph_name).run([nodes], [relationships])
