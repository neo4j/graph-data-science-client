from __future__ import annotations

from typing import Any, Type

from pandas import DataFrame

_TOPOLOGY_COLUMNS = ("sourceNodeId", "targetNodeId", "relationshipType")


class RelationshipsDataFrame(DataFrame):
    """
    A ``DataFrame`` of streamed relationships (with ``sourceNodeId``, ``targetNodeId`` and
    ``relationshipType`` columns, plus a column per streamed relationship property) that
    additionally offers the :meth:`by_rel_type` convenience method for reshaping the
    relationships into a GNN-framework-friendly format.
    """

    @property
    def _constructor(self) -> Type[RelationshipsDataFrame]:
        return RelationshipsDataFrame

    def by_rel_type(self) -> dict[str, list[list[Any]]]:
        """
        Group the relationships by their type.

        Returns
        -------
        dict[str, list[list[Any]]]
            A mapping of relationship type to ``[[source_node_ids], [target_node_ids]]``.
            If relationship properties were streamed, a list of their values is appended
            for each property (in column order), i.e.
            ``[[source_node_ids], [target_node_ids], [property_values], ...]``.
        """
        property_columns = [c for c in self.columns if c not in _TOPOLOGY_COLUMNS]
        value_columns = ["sourceNodeId", "targetNodeId", *property_columns]

        # Materialize each column once as a plain numpy array and index it by the positional
        # group indices. This avoids constructing per-group DataFrame subclasses (and the
        # associated deprecated BlockManager construction path) entirely.
        column_values = {col: self[col].to_numpy() for col in value_columns}
        indices_by_type = self.groupby("relationshipType", observed=True).indices

        output: dict[str, list[list[Any]]] = {}
        for rel_type, positions in indices_by_type.items():
            output[str(rel_type)] = [column_values[col][positions].tolist() for col in value_columns]

        return output
