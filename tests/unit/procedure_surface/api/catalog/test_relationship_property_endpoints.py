from unittest.mock import MagicMock

from graphdatascience.procedure_surface.api.catalog.relationship_property_endpoints import (
    RelationshipPropertyEndpoints,
)
from graphdatascience.procedure_surface.api.catalog.relationships_data_frame import RelationshipsDataFrame
from graphdatascience.procedure_surface.api.catalog.relationships_endpoints import RelationshipsEndpoints


def _delegate() -> MagicMock:
    delegate = MagicMock(spec=RelationshipsEndpoints)
    delegate.stream.return_value = RelationshipsDataFrame(
        {
            "sourceNodeId": [0, 1],
            "targetNodeId": [1, 2],
            "relationshipType": ["REL", "REL"],
            "weight": [1.0, 2.0],
        }
    )
    return delegate


def test_stream_renames_property_column_to_property_value() -> None:
    result = RelationshipPropertyEndpoints(_delegate()).stream(MagicMock(), "weight")

    assert list(result.columns) == ["sourceNodeId", "targetNodeId", "relationshipType", "propertyValue"]
    assert result["propertyValue"].tolist() == [1.0, 2.0]


def test_stream_returns_a_relationships_data_frame() -> None:
    result = RelationshipPropertyEndpoints(_delegate()).stream(MagicMock(), "weight")

    assert isinstance(result, RelationshipsDataFrame)
    assert result.by_rel_type() == {"REL": [[0, 1], [1, 2], [1.0, 2.0]]}


def test_stream_forwards_all_parameters_to_the_relationships_endpoint() -> None:
    delegate = _delegate()
    G = MagicMock()

    RelationshipPropertyEndpoints(delegate).stream(
        G,
        "weight",
        ["REL"],
        concurrency=4,
        sudo=True,
        log_progress=False,
        username="alice",
    )

    delegate.stream.assert_called_once_with(
        G,
        ["REL"],
        ["weight"],
        concurrency=4,
        sudo=True,
        log_progress=False,
        username="alice",
    )
