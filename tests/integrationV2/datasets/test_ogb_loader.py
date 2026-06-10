from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.catalog.catalog_arrow_endpoints import CatalogArrowEndpoints


@pytest.fixture
def catalog(arrow_client: AuthenticatedArrowClient) -> Generator[CatalogArrowEndpoints, None, None]:
    yield CatalogArrowEndpoints(arrow_client)


@pytest.mark.ogb
def test_graph_load_ogbn_arxiv(catalog: CatalogArrowEndpoints) -> None:
    # ogbn-arxiv is a homogeneous dataset for node classification
    graph_name = "ogbn-arxiv"
    try:
        G = catalog.datasets.ogbn.load(graph_name)
        assert G.name() == graph_name
        assert G.node_count() == 169_343
        assert set(G.node_labels()) == {"Train", "Test", "Valid"}
        for lbl in G.node_labels():
            assert set(G.node_properties()[lbl]) == {"features", "classLabel"}
        assert G.relationship_count() == 1_166_243
        assert G.relationship_types() == ["R"]
        assert G.relationship_properties()["R"] == []
    finally:
        catalog.drop(graph_name, fail_if_missing=False)


@pytest.mark.ogb
def test_graph_load_ogbn_mag(catalog: CatalogArrowEndpoints) -> None:
    # ogbn-mag is a heterogeneous dataset for node classification
    graph_name = "ogbn-mag"
    try:
        G = catalog.datasets.ogbn.load(graph_name)
        assert G.name() == graph_name
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
        assert set(G.node_properties()["paper"]) == {"features", "classLabel"}
        assert G.node_properties()["institution"] == []
        assert G.node_properties()["field_of_study"] == []
        assert G.node_properties()["author"] == []
        assert G.relationship_count() == 21_111_007
        assert set(G.relationship_types()) == {"cites", "writes", "affiliated_with", "has_topic"}
        for t in G.relationship_types():
            assert G.relationship_properties()[t] == []
    finally:
        catalog.drop(graph_name, fail_if_missing=False)


@pytest.mark.ogb
def test_graph_load_ogbl_wikikg2(catalog: CatalogArrowEndpoints) -> None:
    # ogbl-wikikg2 is a homogeneous dataset for knowledge graph completion
    graph_name = "ogbl-wikikg2"
    try:
        G = catalog.datasets.ogbl.load(graph_name)
        assert G.name() == graph_name
        assert G.node_count() == 2_500_604
        assert set(G.node_labels()) == {"N"}
        assert G.node_properties()["N"] == []
        # We don't store the negative sampled relationships in the graph.
        assert G.relationship_count() == 17_137_181
        # 535 train, 363 valid, 367 test
        assert len(G.relationship_types()) == 1265
        for t in G.relationship_types():
            assert G.relationship_properties()[t] == []
    finally:
        catalog.drop(graph_name, fail_if_missing=False)


@pytest.mark.ogb
def test_graph_load_ogbl_biokg(catalog: CatalogArrowEndpoints) -> None:
    # ogbl-biokg is a heterogeneous dataset for knowledge graph completion
    graph_name = "ogbl-biokg"
    try:
        G = catalog.datasets.ogbl.load(graph_name)
        assert G.name() == graph_name
        assert G.node_count() == 93_773
        assert set(G.node_labels()) == {"disease", "protein", "drug", "sideeffect", "function"}
        for lbl in G.node_labels():
            assert G.node_properties()[lbl] == []
        assert G.relationship_count() == 5_088_434
        # For each of the train, valid and test sets: number of rel types
        assert len(G.relationship_types()) == 51 * 3
        for t in G.relationship_types():
            assert G.relationship_properties()[t] == ["classLabel"]
    finally:
        catalog.drop(graph_name, fail_if_missing=False)


@pytest.mark.ogb
def test_graph_load_ogbl_ddi(catalog: CatalogArrowEndpoints) -> None:
    # ogbl-ddi is a homogeneous dataset for link prediction
    graph_name = "ogbl-ddi"
    try:
        G = catalog.datasets.ogbl.load(graph_name)
        assert G.name() == graph_name
        assert G.node_count() == 4_267
        assert G.node_labels() == ["N"]
        assert G.node_properties()["N"] == []
        # Positive + negative counts
        assert G.relationship_count() == 1_334_889 + 197_481
        assert set(G.relationship_types()) == {"TRAIN_POS", "VALID_POS", "VALID_NEG", "TEST_POS", "TEST_NEG"}
        assert G.relationship_properties()["TRAIN_POS"] == []
    finally:
        catalog.drop(graph_name, fail_if_missing=False)
