from __future__ import annotations

from unittest.mock import MagicMock

from graphdatascience.procedure_surface.arrow.catalog.graph_backend_arrow import ArrowGraphBackend


def test_drop_delegates_to_graph_ops() -> None:
    backend = ArrowGraphBackend("g", MagicMock())
    backend._graph_ops = MagicMock()
    backend._graph_ops.drop.return_value = None

    backend.drop(fail_if_missing=False)

    backend._graph_ops.drop.assert_called_once_with("g", False)
