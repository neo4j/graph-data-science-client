from typing import Callable

from pandas import DataFrame

from graphdatascience.graph_construction.graph_constructor import GraphConstructor
from graphdatascience.procedure_surface.api.catalog.catalog_endpoints import CatalogEndpoints


class GraphConstructorTestBase:
    """Shared integration tests for all GraphConstructor implementations.

    Subclasses must define:
    - ``graph_name_prefix`` class attribute (str) to avoid name collisions.
    - ``constructor_factory`` pytest fixture returning
      ``(graph_name: str, **kwargs) -> GraphConstructor``.
    - ``catalog`` pytest fixture returning a ``CatalogEndpoints``.
    """

    graph_name_prefix: str

    def test_nodes_only(
        self,
        constructor_factory: Callable[..., GraphConstructor],
        catalog: CatalogEndpoints,
    ) -> None:
        graph_name = f"{self.graph_name_prefix}_nodes_only"
        nodes = DataFrame({"nodeId": [0, 1, 2]})

        try:
            constructor_factory(graph_name).run([nodes], [])

            info = catalog.list(graph_name)
            assert len(info) == 1
            assert info[0].node_count == 3
            assert info[0].relationship_count == 0
        finally:
            catalog.drop(graph_name, fail_if_missing=False)

    def test_nodes_with_labels_and_properties(
        self,
        constructor_factory: Callable[..., GraphConstructor],
        catalog: CatalogEndpoints,
    ) -> None:
        graph_name = f"{self.graph_name_prefix}_nodes_labels_props"
        nodes = DataFrame(
            {
                "nodeId": [0, 1, 2],
                "labels": [["A"], ["B"], ["A"]],
                "score": [1.0, 2.0, 3.0],
            }
        )

        try:
            constructor_factory(graph_name).run([nodes], [])

            info = catalog.list(graph_name)
            assert len(info) == 1
            assert info[0].node_count == 3
            schema = info[0].graph_schema
            assert "A" in schema["nodes"]
            assert "B" in schema["nodes"]
        finally:
            catalog.drop(graph_name, fail_if_missing=False)

    def test_nodes_and_relationships(
        self,
        constructor_factory: Callable[..., GraphConstructor],
        catalog: CatalogEndpoints,
    ) -> None:
        graph_name = f"{self.graph_name_prefix}_nodes_and_rels"
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
            constructor_factory(graph_name).run([nodes], [relationships])

            info = catalog.list(graph_name)
            assert len(info) == 1
            assert info[0].node_count == 3
            assert info[0].relationship_count == 2
            assert "KNOWS" in info[0].graph_schema["relationships"]
        finally:
            catalog.drop(graph_name, fail_if_missing=False)

    def test_multiple_node_dfs(
        self,
        constructor_factory: Callable[..., GraphConstructor],
        catalog: CatalogEndpoints,
    ) -> None:
        graph_name = f"{self.graph_name_prefix}_multi_node_dfs"
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
            constructor_factory(graph_name).run([nodes_a, nodes_b], [relationships])

            info = catalog.list(graph_name)
            assert len(info) == 1
            assert info[0].node_count == 4
            assert info[0].relationship_count == 1
            schema = info[0].graph_schema["nodes"]
            assert "A" in schema
            assert "B" in schema
        finally:
            catalog.drop(graph_name, fail_if_missing=False)

    def test_multiple_relationship_dfs(
        self,
        constructor_factory: Callable[..., GraphConstructor],
        catalog: CatalogEndpoints,
    ) -> None:
        graph_name = f"{self.graph_name_prefix}_multi_rel_dfs"
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
            constructor_factory(graph_name).run([nodes], [rels_a, rels_b])

            info = catalog.list(graph_name)
            assert len(info) == 1
            assert info[0].node_count == 3
            assert info[0].relationship_count == 2
            rel_types = info[0].graph_schema["relationships"]
            assert "REL_A" in rel_types
            assert "REL_B" in rel_types
        finally:
            catalog.drop(graph_name, fail_if_missing=False)

    def test_undirected_relationship_types(
        self,
        constructor_factory: Callable[..., GraphConstructor],
        catalog: CatalogEndpoints,
    ) -> None:
        graph_name = f"{self.graph_name_prefix}_undirected"
        nodes = DataFrame({"nodeId": [0, 1, 2]})
        relationships = DataFrame(
            {
                "sourceNodeId": [0, 1],
                "targetNodeId": [1, 2],
                "relationshipType": ["REL", "REL"],
            }
        )

        try:
            constructor_factory(graph_name, undirected_relationship_types=["REL"]).run([nodes], [relationships])

            info = catalog.list(graph_name)
            assert len(info) == 1
            # undirected means each relationship is stored in both directions
            assert info[0].relationship_count == 4
        finally:
            catalog.drop(graph_name, fail_if_missing=False)
