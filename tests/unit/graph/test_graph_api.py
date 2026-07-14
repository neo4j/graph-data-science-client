from __future__ import annotations

from datetime import datetime

import pytest

from graphdatascience.graph.graph_api import Graph
from graphdatascience.graph.graph_backend import GraphBackend
from graphdatascience.graph.graph_info import GraphInfo, GraphInfoWithDegrees


def _graph_info(**overrides: object) -> GraphInfoWithDegrees:
    row: dict[str, object] = {
        "graphName": "g",
        "database": "neo4j",
        "databaseLocation": "local",
        "configuration": {"jobId": "job-1"},
        "memoryUsage": "1 KiB",
        "sizeInBytes": 1024,
        "nodeCount": 4,
        "relationshipCount": 5,
        "creationTime": datetime(2024, 1, 1),
        "modificationTime": datetime(2024, 1, 2),
        "schemaWithOrientation": {
            "nodes": {"Node": {"x": "Integer"}, "Node2": {}},
            "relationships": {
                "REL": {"properties": {"y": "Float", "z": "Integer"}},
                "REL2": {},
            },
        },
        "density": 0.25,
        "degreeDistribution": {"mean": 1.75},
    }
    row.update(overrides)
    return GraphInfoWithDegrees(**row)


class FakeGraphBackend(GraphBackend):
    """In-memory backend so the Graph delegation/transformation logic can be tested without a database."""

    def __init__(self, info: GraphInfoWithDegrees, exists: bool = True) -> None:
        self._info = info
        self._exists = exists
        self.graph_info_calls = 0
        self.drop_calls: list[bool] = []

    def graph_info(self) -> GraphInfoWithDegrees:
        self.graph_info_calls += 1
        return self._info

    def exists(self) -> bool:
        return self._exists

    def drop(self, fail_if_missing: bool = True) -> GraphInfo | None:
        self.drop_calls.append(fail_if_missing)
        return self._info


@pytest.fixture
def backend() -> FakeGraphBackend:
    return FakeGraphBackend(_graph_info())


@pytest.fixture
def G(backend: FakeGraphBackend) -> Graph:
    return Graph("g", backend)


def test_name(G: Graph) -> None:
    assert G.name() == "g"


def test_configuration(G: Graph) -> None:
    assert G.configuration() == {"jobId": "job-1"}


def test_node_count(G: Graph) -> None:
    assert G.node_count() == 4


def test_relationship_count(G: Graph) -> None:
    assert G.relationship_count() == 5


def test_node_labels(G: Graph) -> None:
    assert set(G.node_labels()) == {"Node", "Node2"}


def test_relationship_types(G: Graph) -> None:
    assert set(G.relationship_types()) == {"REL", "REL2"}


def test_node_properties(G: Graph) -> None:
    assert G.node_properties() == {"Node": ["x"], "Node2": []}


def test_relationship_properties(G: Graph) -> None:
    # REL2 has no "properties" key in the schema, exercising the `.get("properties", {})` fallback
    rel_properties = G.relationship_properties()
    assert set(rel_properties["REL"]) == {"y", "z"}
    assert rel_properties["REL2"] == []


def test_degree_distribution(G: Graph) -> None:
    assert G.degree_distribution() == {"mean": 1.75}


def test_density(G: Graph) -> None:
    assert G.density() == 0.25


def test_memory_usage(G: Graph) -> None:
    assert G.memory_usage() == "1 KiB"


def test_memory_usage_can_be_none() -> None:
    graph = Graph("g", FakeGraphBackend(_graph_info(memoryUsage=None)))
    assert graph.memory_usage() is None


def test_size_in_bytes(G: Graph) -> None:
    assert G.size_in_bytes() == 1024


def test_creation_time(G: Graph) -> None:
    assert G.creation_time() == datetime(2024, 1, 1)


def test_modification_time(G: Graph) -> None:
    assert G.modification_time() == datetime(2024, 1, 2)


def test_exists_true(G: Graph) -> None:
    assert G.exists() is True


def test_exists_false() -> None:
    graph = Graph("g", FakeGraphBackend(_graph_info(), exists=False))
    assert graph.exists() is False


def test_drop_defaults_to_fail_if_missing(backend: FakeGraphBackend, G: Graph) -> None:
    result = G.drop()
    assert result is not None and result.graph_name == "g"
    assert backend.drop_calls == [True]


def test_drop_passes_fail_if_missing_flag(backend: FakeGraphBackend, G: Graph) -> None:
    G.drop(fail_if_missing=False)
    assert backend.drop_calls == [False]


def test_context_manager_drops_on_exit(backend: FakeGraphBackend) -> None:
    with Graph("g", backend) as graph:
        assert graph.name() == "g"
        assert backend.drop_calls == []
    # __exit__ should have triggered a drop
    assert backend.drop_calls == [True]


def test_str(G: Graph) -> None:
    assert str(G) == "Graph(name=g, node_count=4, relationship_count=5)"


def test_repr(G: Graph) -> None:
    representation = repr(G)
    assert representation.startswith("Graph(")
    assert "'memory_usage'" in representation
    assert "'graph_name'" in representation
