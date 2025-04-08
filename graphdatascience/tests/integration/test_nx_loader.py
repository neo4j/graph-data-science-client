import pytest

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.server_version.server_version import ServerVersion

GRAPH_NAME = "banana"

pytest.importorskip("networkx")

import networkx as nx  # noqa: E402


@pytest.fixture
def undirected_nx_G() -> nx.Graph:
    nx_G = nx.Graph()

    nx_G.add_node(1, labels=["N"], foo=1)
    nx_G.add_node(42, labels=["N", "M"], foo=2)
    nx_G.add_node(1337, labels=["N"], foo=3)
    nx_G.add_edge(1, 42, relationshipType="R", weight=0.4)
    nx_G.add_edge(1, 1337, relationshipType="R", weight=1.4)
    nx_G.add_edge(42, 1337, relationshipType="R", weight=1.1)

    return nx_G


@pytest.fixture
def directed_nx_G() -> nx.Graph:
    nx_G = nx.DiGraph()

    nx_G.add_node(1, labels=["N"], foo=1)
    nx_G.add_node(42, labels=["N", "M"], foo=2)
    nx_G.add_node(1337, labels=["N"], foo=3)
    nx_G.add_edge(1, 42, relationshipType="R", weight=0.4)
    nx_G.add_edge(1, 1337, relationshipType="R", weight=1.4)
    nx_G.add_edge(42, 1337, relationshipType="R", weight=1.1)

    return nx_G


@pytest.fixture
def graph_name(request: pytest.FixtureRequest) -> str:
    GRAPH_NAME = request.function.__name__
    return str(GRAPH_NAME)


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_undirected_nx(gds: GraphDataScience, undirected_nx_G: nx.Graph, graph_name: str) -> None:
    with gds.graph.networkx.load(undirected_nx_G, graph_name) as G:
        assert G.name() == graph_name

        assert G.node_count() == 3
        assert set(G.node_labels()) == {"N", "M"}
        assert G.node_properties("N") == ["foo"]
        assert G.node_properties("M") == ["foo"]

        assert G.relationship_count() == 6
        assert G.relationship_types() == ["R"]
        assert G.relationship_properties("R") == ["weight"]

        G.drop()


@pytest.mark.filterwarnings("ignore: GDS Enterprise users can use Apache Arrow")
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_undirected_nx_without_Arrow(
    gds_without_arrow: GraphDataScience, undirected_nx_G: nx.Graph, graph_name: str
) -> None:
    with gds_without_arrow.graph.networkx.load(undirected_nx_G, graph_name) as G:
        assert G.name() == graph_name

        assert G.node_count() == 3
        assert set(G.node_labels()) == {"N", "M"}
        assert G.node_properties("N") == ["foo"]
        assert G.node_properties("M") == ["foo"]

        assert G.relationship_count() == 6
        assert set(G.relationship_types()) == {"R"}
        assert G.relationship_properties("R") == ["weight"]

        G.drop()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_directed_nx(gds: GraphDataScience, directed_nx_G: nx.Graph, graph_name: str) -> None:
    with gds.graph.networkx.load(directed_nx_G, graph_name) as G:
        assert G.name() == graph_name

        assert G.node_count() == 3
        assert set(G.node_labels()) == {"N", "M"}
        assert G.node_properties("N") == ["foo"]
        assert G.node_properties("M") == ["foo"]

        assert G.relationship_count() == 3
        assert set(G.relationship_types()) == {"R"}
        assert G.relationship_properties("R") == ["weight"]

        G.drop()


@pytest.mark.filterwarnings("ignore: GDS Enterprise users can use Apache Arrow")
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_directed_nx_without_Arrow(
    gds_without_arrow: GraphDataScience, directed_nx_G: nx.Graph, graph_name: str
) -> None:
    with gds_without_arrow.graph.networkx.load(directed_nx_G, graph_name) as G:
        assert G.name() == graph_name

        assert G.node_count() == 3
        assert set(G.node_labels()) == {"N", "M"}
        assert G.node_properties("N") == ["foo"]
        assert G.node_properties("M") == ["foo"]

        assert G.relationship_count() == 3
        assert set(G.relationship_types()) == {"R"}
        assert G.relationship_properties("R") == ["weight"]

        G.drop()
