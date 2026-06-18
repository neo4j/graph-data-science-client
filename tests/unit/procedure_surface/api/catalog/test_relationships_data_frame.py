from graphdatascience.procedure_surface.api.catalog.relationships_data_frame import RelationshipsDataFrame


def test_by_rel_type_topology_only() -> None:
    df = RelationshipsDataFrame(
        {
            "sourceNodeId": [0, 1, 2, 3],
            "targetNodeId": [1, 2, 3, 0],
            "relationshipType": ["REL", "REL", "REL", "REL"],
        }
    )

    assert df.by_rel_type() == {"REL": [[0, 1, 2, 3], [1, 2, 3, 0]]}


def test_by_rel_type_multiple_types() -> None:
    df = RelationshipsDataFrame(
        {
            "sourceNodeId": [0, 1, 5],
            "targetNodeId": [1, 2, 6],
            "relationshipType": ["REL", "REL", "OTHER"],
        }
    )

    result = df.by_rel_type()

    assert result == {"REL": [[0, 1], [1, 2]], "OTHER": [[5], [6]]}


def test_by_rel_type_includes_properties() -> None:
    df = RelationshipsDataFrame(
        {
            "sourceNodeId": [0, 1],
            "targetNodeId": [1, 2],
            "relationshipType": ["REL", "REL"],
            "weight": [1.5, 2.5],
            "cost": [10, 20],
        }
    )

    # Property columns are appended after sources/targets, in column order.
    assert df.by_rel_type() == {"REL": [[0, 1], [1, 2], [1.5, 2.5], [10, 20]]}


def test_constructor_preserves_subclass_on_slicing() -> None:
    df = RelationshipsDataFrame(
        {
            "sourceNodeId": [0],
            "targetNodeId": [1],
            "relationshipType": ["REL"],
            "weight": [1.0],
        }
    )

    sliced = df[["sourceNodeId", "targetNodeId", "relationshipType"]]

    assert isinstance(sliced, RelationshipsDataFrame)
    assert sliced.by_rel_type() == {"REL": [[0], [1]]}
