from unittest.mock import MagicMock

from graphdatascience.procedure_surface.api.catalog.relationship_properties_endpoints import (
    RelationshipPropertiesEndpoints,
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
            "cost": [10.0, 20.0],
        }
    )
    return delegate


def test_stream_keeps_one_column_per_property() -> None:
    result = RelationshipPropertiesEndpoints(_delegate()).stream(MagicMock(), ["weight", "cost"])

    assert list(result.columns) == ["sourceNodeId", "targetNodeId", "relationshipType", "weight", "cost"]


def test_stream_returns_a_relationships_data_frame() -> None:
    result = RelationshipPropertiesEndpoints(_delegate()).stream(MagicMock(), ["weight", "cost"])

    assert isinstance(result, RelationshipsDataFrame)
    assert result.by_rel_type() == {"REL": [[0, 1], [1, 2], [1.0, 2.0], [10.0, 20.0]]}


def test_write_forwards_all_parameters_to_the_relationships_endpoint() -> None:
    delegate = _delegate()
    G = MagicMock()

    result = RelationshipPropertiesEndpoints(delegate).write(
        G,
        "REL",
        ["weight", "cost"],
        concurrency=4,
        write_concurrency=2,
        sudo=True,
        log_progress=False,
        username="alice",
        job_id="job-1",
    )

    delegate.write.assert_called_once_with(
        G,
        "REL",
        ["weight", "cost"],
        concurrency=4,
        write_concurrency=2,
        sudo=True,
        log_progress=False,
        username="alice",
        job_id="job-1",
    )
    assert result is delegate.write.return_value


def test_stream_forwards_all_parameters_to_the_relationships_endpoint() -> None:
    delegate = _delegate()
    G = MagicMock()

    RelationshipPropertiesEndpoints(delegate).stream(
        G,
        ["weight", "cost"],
        ["REL"],
        concurrency=4,
        sudo=True,
        log_progress=False,
        username="alice",
    )

    delegate.stream.assert_called_once_with(
        G,
        ["REL"],
        ["weight", "cost"],
        concurrency=4,
        sudo=True,
        log_progress=False,
        username="alice",
    )
