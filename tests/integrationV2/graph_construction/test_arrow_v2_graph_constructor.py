from typing import Generator

import pytest
from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph_construction.arrow_v2_graph_constructor import ArrowV2GraphConstructor
from graphdatascience.procedure_surface.arrow.catalog.catalog_arrow_endpoints import CatalogArrowEndpoints


@pytest.fixture
def catalog(arrow_client: AuthenticatedArrowClient) -> Generator[CatalogArrowEndpoints, None, None]:
    yield CatalogArrowEndpoints(arrow_client)


def _constructor(arrow_client: AuthenticatedArrowClient, graph_name: str, **kwargs: object) -> ArrowV2GraphConstructor:
    return ArrowV2GraphConstructor(
        authenticated_arrow_client=arrow_client,
        graph_name=graph_name,
        show_progress=False,
        **kwargs,  # type: ignore[arg-type]
    )


def test_nodes_only(arrow_client: AuthenticatedArrowClient, catalog: CatalogArrowEndpoints) -> None:
    graph_name = "arrow_v2_nodes_only"
    nodes = DataFrame({"nodeId": [0, 1, 2]})

    try:
        _constructor(arrow_client, graph_name).run([nodes], [])

        info = catalog.list(graph_name)
        assert len(info) == 1
        assert info[0].node_count == 3
        assert info[0].relationship_count == 0
    finally:
        catalog.drop(graph_name, fail_if_missing=False)


def test_nodes_with_labels_and_properties(
    arrow_client: AuthenticatedArrowClient, catalog: CatalogArrowEndpoints
) -> None:
    graph_name = "arrow_v2_nodes_labels_props"
    nodes = DataFrame(
        {
            "nodeId": [0, 1, 2],
            "labels": [["A"], ["B"], ["A"]],
            "score": [1.0, 2.0, 3.0],
        }
    )

    try:
        _constructor(arrow_client, graph_name).run([nodes], [])

        info = catalog.list(graph_name)
        assert len(info) == 1
        assert info[0].node_count == 3
        schema = info[0].graph_schema
        assert "A" in schema["nodes"]
        assert "B" in schema["nodes"]
    finally:
        catalog.drop(graph_name, fail_if_missing=False)


def test_nodes_and_relationships(arrow_client: AuthenticatedArrowClient, catalog: CatalogArrowEndpoints) -> None:
    graph_name = "arrow_v2_nodes_and_rels"
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
        _constructor(arrow_client, graph_name).run([nodes], [relationships])

        info = catalog.list(graph_name)
        assert len(info) == 1
        assert info[0].node_count == 3
        assert info[0].relationship_count == 2
        assert "KNOWS" in info[0].graph_schema["relationships"]
    finally:
        catalog.drop(graph_name, fail_if_missing=False)


def test_multiple_node_dfs(arrow_client: AuthenticatedArrowClient, catalog: CatalogArrowEndpoints) -> None:
    graph_name = "arrow_v2_multi_node_dfs"
    nodes_a = DataFrame({"nodeId": [0, 1], "labels": [["A"], ["A"]]})
    nodes_b = DataFrame({"nodeId": [2, 3], "labels": [["B"], ["B"]]})
    relationships = DataFrame(
        {
            "sourceNodeId": [0],
            "targetNodeId": [2],
            "relationshipType": ["REL"],
        }
    )

    try:
        _constructor(arrow_client, graph_name).run([nodes_a, nodes_b], [relationships])

        info = catalog.list(graph_name)
        assert len(info) == 1
        assert info[0].node_count == 4
        assert info[0].relationship_count == 1
        schema = info[0].graph_schema["nodes"]
        assert "A" in schema
        assert "B" in schema
    finally:
        catalog.drop(graph_name, fail_if_missing=False)


def test_undirected_relationship_types(arrow_client: AuthenticatedArrowClient, catalog: CatalogArrowEndpoints) -> None:
    graph_name = "arrow_v2_undirected"
    nodes = DataFrame({"nodeId": [0, 1, 2]})
    relationships = DataFrame(
        {
            "sourceNodeId": [0, 1],
            "targetNodeId": [1, 2],
            "relationshipType": ["REL", "REL"],
        }
    )

    try:
        _constructor(arrow_client, graph_name, undirected_relationship_types=["REL"]).run([nodes], [relationships])

        info = catalog.list(graph_name)
        assert len(info) == 1
        # undirected means each relationship is stored in both directions
        assert info[0].relationship_count == 4
    finally:
        catalog.drop(graph_name, fail_if_missing=False)


def test_multiple_relationship_dfs(arrow_client: AuthenticatedArrowClient, catalog: CatalogArrowEndpoints) -> None:
    graph_name = "arrow_v2_multi_rel_dfs"
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
        _constructor(arrow_client, graph_name).run([nodes], [rels_a, rels_b])

        info = catalog.list(graph_name)
        assert len(info) == 1
        assert info[0].node_count == 3
        assert info[0].relationship_count == 2
        rel_types = info[0].graph_schema["relationships"]
        assert "REL_A" in rel_types
        assert "REL_B" in rel_types
    finally:
        catalog.drop(graph_name, fail_if_missing=False)
