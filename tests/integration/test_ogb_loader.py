import pytest

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.server_version.server_version import ServerVersion


@pytest.mark.ogb
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_load_ogbn_arxiv(gds: GraphDataScience) -> None:
    # ogbn-arxiv is a homogeneous dataset for node classification

    G = gds.graph.ogbn.load("ogbn-arxiv")

    assert G.name() == "ogbn-arxiv"
    assert G.node_count() == 169_343
    assert set(G.node_labels()) == {"Train", "Test", "Valid"}
    for lbl in G.node_labels():
        assert set(G.node_properties()[lbl]) == {"features", "classLabel"}  # type: ignore
    assert G.relationship_count() == 1_166_243
    assert G.relationship_types() == ["R"]
    assert G.relationship_properties()["R"] == []  # type: ignore

    G.drop()


@pytest.mark.ogb
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_load_ogbn_mag(gds: GraphDataScience) -> None:
    # ogbn-mag is a heterogeneous dataset for node classification

    G = gds.graph.ogbn.load("ogbn-mag")

    assert G.name() == "ogbn-mag"
    assert G.node_count() == 1_939_743
    assert set(G.node_labels()) == {
        "Train",
        "Test",
        "Valid",
        "institution",
        "field_of_study",
        "paper",
        "author",
    }
    assert set(G.node_properties()["paper"]) == {"features", "classLabel"}  # type: ignore
    assert G.node_properties()["institution"] == []  # type: ignore
    assert G.node_properties()["field_of_study"] == []  # type: ignore
    assert G.node_properties()["author"] == []  # type: ignore
    assert G.relationship_count() == 21_111_007
    assert set(G.relationship_types()) == {"cites", "writes", "affiliated_with", "has_topic"}
    for t in G.relationship_types():
        assert G.relationship_properties()[t] == []  # type: ignore

    G.drop()


@pytest.mark.ogb
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_load_ogbl_wikikg2(gds: GraphDataScience) -> None:
    # ogbl-wikikg2 is a homogenous dataset for knowledge graph completion

    G = gds.graph.ogbl.load("ogbl-wikikg2")

    assert G.name() == "ogbl-wikikg2"
    assert G.node_count() == 2_500_604
    assert set(G.node_labels()) == {"N"}
    assert G.node_properties()["N"] == []  # type: ignore

    # We don't store the negative sampled relationships in the graph.
    assert G.relationship_count() == 17_137_181
    # 535 train, 363 valid, 367 test
    assert len(G.relationship_types()) == 1265
    for t in G.relationship_types():
        assert G.relationship_properties()[t] == []  # type: ignore

    G.drop()


@pytest.mark.ogb
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_load_ogbl_biokg(gds: GraphDataScience) -> None:
    # ogbl-biokg is a heterogeneous dataset for knowledge graph completion

    G = gds.graph.ogbl.load("ogbl-biokg")

    assert G.name() == "ogbl-biokg"
    assert G.node_count() == 93_773
    assert set(G.node_labels()) == {"disease", "protein", "drug", "sideeffect", "function"}
    for lbl in G.node_labels():
        assert G.node_properties()[lbl] == []  # type: ignore
    assert G.relationship_count() == 5_088_434
    # For each of the train, valid and test sets: number of rel types
    assert len(G.relationship_types()) == 51 * 3
    for t in G.relationship_types():
        assert G.relationship_properties()[t] == ["classLabel"]  # type: ignore

    G.drop()


@pytest.mark.ogb
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_load_ogbl_ddi(gds: GraphDataScience) -> None:
    # ogbl-ddi is a homogeneous dataset for link prediction

    G = gds.graph.ogbl.load("ogbl-ddi")

    assert G.name() == "ogbl-ddi"
    assert G.node_count() == 4_267
    assert G.node_labels() == ["N"]
    assert G.node_properties()["N"] == []  # type: ignore
    # Positive + negative counts
    assert G.relationship_count() == 1_334_889 + 197_481
    assert set(G.relationship_types()) == {"TRAIN_POS", "VALID_POS", "VALID_NEG", "TEST_POS", "TEST_NEG"}
    assert G.relationship_properties()["TRAIN_POS"] == []  # type: ignore

    G.drop()
