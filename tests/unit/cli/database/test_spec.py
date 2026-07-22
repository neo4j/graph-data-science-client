import pytest
from gds_cli.database.graph import NodeIdMapping
from gds_cli.database.spec import graph_from_spec, random_graph_config_from_spec


def test_graph_from_spec_basic() -> None:
    spec = {
        "kind": "random",
        "nodes": {
            "Person": {
                "count": 10,
                "properties": {"age": {"type": "uniform_int", "low": 18, "high": 80}},
            },
            "Company": {"count": 5},
        },
        "relationships": [
            {
                "source": "Person",
                "type": "WORKS_AT",
                "target": "Company",
                "count": 8,
                "generator": {"type": "powerlaw", "alpha": 0.7},
                "properties": {"since": {"type": "uniform_int", "low": 2000, "high": 2024}},
            }
        ],
    }

    graph = graph_from_spec(spec)

    assert len(graph.node_dfs["Person"]) == 10
    assert len(graph.node_dfs["Company"]) == 5
    assert "age" in graph.node_dfs["Person"].columns
    assert len(graph.rel_dfs[("Person", "WORKS_AT", "Company")]) == 8


def test_vector_property_via_dim() -> None:
    """A numeric property generator with `dim` emits a per-node vector of native floats/ints."""
    spec = {
        "kind": "random",
        "nodes": {
            "Event": {
                "count": 6,
                "properties": {
                    "features": {"type": "gaussian", "mean": 0.2, "std": 0.1, "dim": 4},
                    "counts": {"type": "uniform_int", "low": 0, "high": 5, "dim": 3},
                },
            }
        },
        "relationships": [],
    }

    events = graph_from_spec(spec).node_dfs["Event"]

    feat = events["features"].iloc[0]
    assert isinstance(feat, list) and len(feat) == 4 and all(isinstance(x, float) for x in feat)
    cnt = events["counts"].iloc[0]
    assert isinstance(cnt, list) and len(cnt) == 3 and all(isinstance(x, int) for x in cnt)


def test_random_graph_config_defaults_to_globally_unique_for_multi_label() -> None:
    spec = {
        "nodes": {"A": {"count": 1}, "B": {"count": 1}},
        "relationships": [],
    }

    config = random_graph_config_from_spec(spec)

    assert config.nodeIdMapping == NodeIdMapping.GLOBALLY_UNIQUE


def test_random_graph_config_missing_nodes_raises() -> None:
    with pytest.raises(ValueError, match="non-empty 'nodes' mapping"):
        random_graph_config_from_spec({"nodes": {}})


def test_random_graph_config_missing_count_raises() -> None:
    with pytest.raises(ValueError, match="missing 'count'"):
        random_graph_config_from_spec({"nodes": {"Person": {}}})


def test_random_graph_config_unknown_property_generator_raises() -> None:
    spec = {"nodes": {"Person": {"count": 1, "properties": {"x": {"type": "bogus"}}}}}

    with pytest.raises(ValueError, match="Unknown property generator type"):
        random_graph_config_from_spec(spec)


def test_random_graph_config_unknown_relationship_generator_raises() -> None:
    spec = {
        "nodes": {"Person": {"count": 1}},
        "relationships": [
            {"source": "Person", "type": "R", "target": "Person", "count": 1, "generator": {"type": "bogus"}}
        ],
    }

    with pytest.raises(ValueError, match="Unknown relationship generator type"):
        random_graph_config_from_spec(spec)


def test_random_graph_config_unknown_node_id_mapping_raises() -> None:
    spec = {"nodes": {"Person": {"count": 1}}, "node_id_mapping": "bogus"}

    with pytest.raises(ValueError, match="Unknown node_id_mapping"):
        random_graph_config_from_spec(spec)
