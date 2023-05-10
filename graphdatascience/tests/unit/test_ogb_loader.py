from typing import Any, Dict

import numpy as np
import numpy.typing as npt
from pandas import Series

from graphdatascience.graph.ogb_loader import (
    HeterogeneousOGBGraph,
    HeterogeneousOGBLDataset,
    HeterogeneousOGBNDataset,
    HomogeneousOGBGraph,
    HomogeneousOGBLDataset,
    HomogeneousOGBNDataset,
)
from graphdatascience.graph_data_science import GraphDataScience

HOMOGENEOUS_EDGE_INDEX = [[0, 1], [2, 1]]
HOMOGENEOUS_NODE_FEAT = [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]]
HOMOGENEOUS_NUM_NODES = 3
HOMOGENEOUS_EXPECTED_NODES = list(range(HOMOGENEOUS_NUM_NODES))
HOMOGENEOUS_CLASS_LABELS = [0, 1, 0]


class HomoOBGNTestDataset(HomogeneousOGBNDataset):
    def __init__(self) -> None:
        self.graph: HomogeneousOGBGraph = {
            "node_feat": np.array(HOMOGENEOUS_NODE_FEAT),
            "edge_index": np.array(HOMOGENEOUS_EDGE_INDEX),
            "num_nodes": HOMOGENEOUS_NUM_NODES,
        }
        self.labels = np.array([[cl] for cl in HOMOGENEOUS_CLASS_LABELS])
        self.meta_info = Series({"has_edge_attr": "False"})

    def get_idx_split(self) -> Dict[str, npt.NDArray[np.int64]]:
        return {"train": np.array([0]), "valid": np.array([1]), "test": np.array([2])}


class HomoOBGLTestDataset(HomogeneousOGBLDataset):
    def __init__(self) -> None:
        self.graph: HomogeneousOGBGraph = {
            "node_feat": np.array(HOMOGENEOUS_NODE_FEAT),
            "edge_index": np.array(HOMOGENEOUS_EDGE_INDEX),
            "num_nodes": HOMOGENEOUS_NUM_NODES,
        }
        self.meta_info = Series({"has_edge_attr": "False"})
        self.name = "ogb_test_graph"

    def get_edge_split(self) -> Dict[str, Dict[str, npt.NDArray[np.int64]]]:
        return {
            "train": {"edge": np.array([[0, 1]]), "edge_neg": np.array([[0, 2]])},
            "valid": {"edge": np.array([[1, 2]]), "edge_neg": np.array([[2, 1]])},
            "test": {"edge_neg": np.array([[1, 1]])},
        }


HETEROGENEOUS_EDGE_INDEX = {("A", "R", "B"): np.array([[0], [0]]), ("B", "R2", "C"): np.array([[0], [0]])}
HETEROGENEOUS_NODE_FEAT = {"A": np.array([[1.0]]), "C": np.array([[42.1337]])}
HETEROGENEOUS_NUM_NODES = {"A": 1, "B": 1, "C": 1}
HETEROGENEOUS_CLASS_LABELS = {"A": np.array([[5]])}


class HeteroOBGNTestDataset(HeterogeneousOGBNDataset):
    def __init__(self) -> None:
        self.graph: HeterogeneousOGBGraph = {
            "node_feat_dict": HETEROGENEOUS_NODE_FEAT,
            "edge_index_dict": HETEROGENEOUS_EDGE_INDEX,
            "num_nodes_dict": HETEROGENEOUS_NUM_NODES,
        }
        self.labels = HETEROGENEOUS_CLASS_LABELS
        self.meta_info = Series({"has_edge_attr": "False"})

    def get_idx_split(self) -> Dict[str, Dict[str, npt.NDArray[np.int64]]]:
        return {"train": {"A": np.array([0])}, "valid": {"A": np.array([])}, "test": {"A": np.array([])}}


class HeteroOBGLTestDataset(HeterogeneousOGBLDataset):
    def __init__(self) -> None:
        self.graph: HeterogeneousOGBGraph = {
            "node_feat_dict": HETEROGENEOUS_NODE_FEAT,
            "edge_index_dict": HETEROGENEOUS_EDGE_INDEX,
            "num_nodes_dict": HETEROGENEOUS_NUM_NODES,
        }
        self.meta_info = Series({"has_edge_attr": "False"})

    def get_edge_split(self) -> Dict[str, Dict[str, Any]]:
        return {
            "train": {"head_type": ["A"], "head": [0], "relation": [0], "tail_type": ["B"], "tail": [0]},
            "valid": {"head_type": ["B"], "head": [0], "relation": [1], "tail_type": ["C"], "tail": [0]},
            "test": {"head_type": [], "head": [], "relation": [], "tail_type": [], "tail": []},
        }


def test_ogbn_parse_homogeneous(gds: GraphDataScience) -> None:
    nodes, rels = gds.graph.ogbn._parse_homogeneous(HomoOBGNTestDataset())

    assert len(nodes) == 1
    assert nodes[0]["nodeId"].tolist() == HOMOGENEOUS_EXPECTED_NODES
    assert nodes[0]["features"].tolist(), HOMOGENEOUS_NODE_FEAT
    assert nodes[0]["classLabel"].tolist() == HOMOGENEOUS_CLASS_LABELS
    assert nodes[0]["labels"].tolist() == ["Train", "Valid", "Test"]

    assert len(rels) == 1
    assert rels[0]["sourceNodeId"].tolist() == HOMOGENEOUS_EDGE_INDEX[0]
    assert rels[0]["targetNodeId"].tolist() == HOMOGENEOUS_EDGE_INDEX[1]
    assert rels[0]["relationshipType"].tolist() == ["R"] * len(HOMOGENEOUS_EDGE_INDEX[0])


def test_ogbl_parse_homogeneous(gds: GraphDataScience) -> None:
    nodes, rels = gds.graph.ogbl._parse_homogeneous(HomoOBGLTestDataset())

    assert len(nodes) == 1
    assert nodes[0]["nodeId"].tolist() == HOMOGENEOUS_EXPECTED_NODES
    assert nodes[0]["features"].tolist() == HOMOGENEOUS_NODE_FEAT
    assert nodes[0]["labels"].tolist() == ["N"] * HOMOGENEOUS_NUM_NODES

    assert len(rels) == 1
    assert rels[0]["sourceNodeId"].tolist() == [0, 0, 1, 2, 1]
    assert rels[0]["targetNodeId"].tolist() == [1, 2, 2, 1, 1]
    assert rels[0]["relationshipType"].tolist() == ["TRAIN_POS", "TRAIN_NEG", "VALID_POS", "VALID_NEG", "TEST_NEG"]


def test_ogbn_parse_heterogeneous(gds: GraphDataScience) -> None:
    nodes, rels = gds.graph.ogbn._parse_heterogeneous(HeteroOBGNTestDataset())

    assert len(nodes) == 3

    assert nodes[0]["nodeId"].tolist() == [0]
    assert nodes[0]["features"].tolist() == HETEROGENEOUS_NODE_FEAT["A"].tolist()
    assert nodes[0]["classLabel"].tolist() == [cl[0] for cl in HETEROGENEOUS_CLASS_LABELS["A"]]
    assert nodes[0]["labels"].tolist() == [["A", "Train"]]

    assert nodes[1]["nodeId"].tolist() == [1]
    assert "features" not in nodes[1]
    assert "classLabel" not in nodes[1]
    assert nodes[1]["labels"].tolist() == ["B"]

    assert nodes[2]["nodeId"].tolist() == [2]
    assert nodes[2]["features"].tolist() == HETEROGENEOUS_NODE_FEAT["C"].tolist()
    assert "classLabel" not in nodes[2]
    assert nodes[2]["labels"].tolist() == ["C"]

    assert len(rels) == 2

    assert rels[0]["sourceNodeId"].tolist() == [0]
    assert rels[0]["targetNodeId"].tolist() == [1]
    assert rels[0]["relationshipType"].tolist() == ["R"] * len(HETEROGENEOUS_EDGE_INDEX[("A", "R", "B")][0])

    assert rels[1]["sourceNodeId"].tolist() == [1]
    assert rels[1]["targetNodeId"].tolist() == [2]
    assert rels[1]["relationshipType"].tolist() == ["R2"] * len(HETEROGENEOUS_EDGE_INDEX[("B", "R2", "C")][0])


def test_ogbl_parse_heterogeneous(gds: GraphDataScience) -> None:
    nodes, rels = gds.graph.ogbl._parse_heterogeneous(HeteroOBGLTestDataset())

    assert len(nodes) == 3

    assert nodes[0]["nodeId"].tolist() == [0]
    assert nodes[0]["features"].tolist() == HETEROGENEOUS_NODE_FEAT["A"].tolist()
    assert nodes[0]["labels"].tolist(), ["A"]

    assert nodes[1]["nodeId"].tolist() == [1]
    assert "features" not in nodes[1]
    assert nodes[1]["labels"].tolist() == ["B"]

    assert nodes[2]["nodeId"].tolist() == [2]
    assert nodes[2]["features"].tolist() == HETEROGENEOUS_NODE_FEAT["C"].tolist()
    assert nodes[2]["labels"].tolist() == ["C"]

    assert len(rels) == 3

    assert rels[0]["sourceNodeId"].tolist() == [0]
    assert rels[0]["targetNodeId"].tolist() == [1]
    assert rels[0]["relationshipType"].tolist() == ["R_TRAIN"] * len(HETEROGENEOUS_EDGE_INDEX[("A", "R", "B")][0])
    assert rels[0]["classLabel"].tolist() == [0] * len(HETEROGENEOUS_EDGE_INDEX[("A", "R", "B")][0])

    assert rels[1]["sourceNodeId"].tolist() == [1]
    assert rels[1]["targetNodeId"].tolist() == [2]
    assert rels[1]["relationshipType"].tolist() == ["R2_VALID"] * len(HETEROGENEOUS_EDGE_INDEX[("B", "R2", "C")][0])
    assert rels[1]["classLabel"].tolist() == [1] * len(HETEROGENEOUS_EDGE_INDEX[("B", "R2", "C")][0])

    assert len(rels[2]) == 0
