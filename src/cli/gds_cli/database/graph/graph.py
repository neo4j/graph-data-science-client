"""In-memory graph model: dict-of-DataFrames keyed by node label / rel triplet.

Vendored from ``data_science_python_runtime.graph.graph`` and trimmed for the
prototype: the ``torch`` / ``torch_geometric`` methods (``get_pyg_data``,
``get_neighbors``, ``_fill_neighs``) are dropped so the CLI stays dependency-light.
"""

import enum
from typing import Any

import numpy as np
from pandas import DataFrame


class NodeIdMapping(enum.Enum):
    """How node IDs should be remapped."""

    # Node IDs become unique only within a label after mapping
    LABEL_UNIQUE = 0
    # Node IDs become globally unique across all labels after mapping
    GLOBALLY_UNIQUE = 1


class Graph:
    def __init__(
        self,
        node_dfs: dict[str, DataFrame],  # label -> nodes
        rel_dfs: dict[tuple[str, str, str], DataFrame],  # (source_label, type, target_label) -> relationships
    ):
        self.node_dfs = node_dfs
        self.rel_dfs = rel_dfs
        self.node_id_to_internal: dict[str, DataFrame] = {}
        self.node_id_to_original: dict[str, np.ndarray | dict[int, Any]] = {}

        self._validate_node_id_uniqueness()
        self._compute_rel_id_map()

    def _validate_node_id_uniqueness(self) -> None:
        bad_labels = set()
        for label, df in self.node_dfs.items():
            if df.shape[0] > len(df["nodeId"].unique()):
                bad_labels.add(label)

        if len(bad_labels) > 0:
            raise ValueError(f"Node IDs are not unique for labels {[set(label) for label in bad_labels]}")

    def remap_node_ids(self, node_id_mapping: NodeIdMapping) -> None:
        if node_id_mapping == NodeIdMapping.LABEL_UNIQUE:
            self._remap_label_unique()
        elif node_id_mapping == NodeIdMapping.GLOBALLY_UNIQUE:
            self._remap_globally_unique()
        else:
            # Should never happen
            raise RuntimeError(f"Unknown node_id_mapping: {node_id_mapping}")

    def _remap_label_unique(self) -> None:
        for label, df in self.node_dfs.items():
            internal_ids = range(df.shape[0])
            self.node_id_to_internal[label] = df[["nodeId"]].copy()
            self.node_id_to_internal[label]["internalId"] = internal_ids

            self.node_id_to_original[label] = df["nodeId"].to_numpy()

            df["nodeId"] = internal_ids

        for rel_triple, df in self.rel_dfs.items():
            source_label, _, target_label = rel_triple

            df = df.merge(self.node_id_to_internal[source_label], left_on="sourceNodeId", right_on="nodeId")
            df.drop(["sourceNodeId", "nodeId"], axis=1, inplace=True)
            df.rename({"internalId": "sourceNodeId"}, axis=1, inplace=True)
            df = df.merge(self.node_id_to_internal[target_label], left_on="targetNodeId", right_on="nodeId")
            df.drop(["targetNodeId", "nodeId"], axis=1, inplace=True)
            df.rename({"internalId": "targetNodeId"}, axis=1, inplace=True)
            self.rel_dfs[rel_triple] = df

    def _remap_globally_unique(self) -> None:
        offset = 0
        for label, df in self.node_dfs.items():
            internal_ids = range(offset, offset + df.shape[0])
            self.node_id_to_internal[label] = df[["nodeId"]].copy()
            self.node_id_to_internal[label]["internalId"] = internal_ids
            self.node_id_to_original[label] = dict(zip(internal_ids, df["nodeId"]))
            offset += df.shape[0]

            df["nodeId"] = internal_ids

        for rel_triple, df in self.rel_dfs.items():
            source_label, _, target_label = rel_triple

            df = df.merge(self.node_id_to_internal[source_label], left_on="sourceNodeId", right_on="nodeId")
            df.drop(["sourceNodeId", "nodeId"], axis=1, inplace=True)
            df.rename({"internalId": "sourceNodeId"}, axis=1, inplace=True)
            df = df.merge(self.node_id_to_internal[target_label], left_on="targetNodeId", right_on="nodeId")
            df.drop(["targetNodeId", "nodeId"], axis=1, inplace=True)
            df.rename({"internalId": "targetNodeId"}, axis=1, inplace=True)
            self.rel_dfs[rel_triple] = df

    def _compute_rel_id_map(self) -> None:
        self.rel_id_to_id: dict[tuple[str, str, str], int] = {}
        for key in self.rel_dfs:
            self.rel_id_to_id[key] = len(self.rel_id_to_id)
