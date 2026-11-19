from typing import Any
from unittest import mock

import pandas as pd
import pytest

from graphdatascience.procedure_surface.cypher.node_embedding.graphsage_train_cypher_endpoints import (
    GraphSageTrainCypherEndpoints,
)
from graphdatascience.versions import ServerVersion

_SUPPORTED = ServerVersion(2026, 11, 0)
_UNSUPPORTED = ServerVersion(2025, 9, 0)


def _query_runner(server_version: ServerVersion) -> mock.Mock:
    query_runner = mock.Mock()
    query_runner.server_version.return_value = server_version
    query_runner.call_procedure.return_value.iloc = mock.MagicMock()
    query_runner.call_procedure.return_value.iloc.__getitem__.return_value = pd.Series(
        {
            "modelInfo": {"modelName": "model"},
            "configuration": {},
            "trainMillis": 1,
        }
    )
    return query_runner


def _train_config(query_runner: mock.Mock, patience: int | None) -> dict[str, Any]:
    graph = mock.Mock()
    graph.name.return_value = "g"

    GraphSageTrainCypherEndpoints(query_runner)(
        G=graph,
        model_name="model",
        feature_properties=["embedding"],
        patience=patience,
    )

    config: dict[str, Any] = query_runner.call_procedure.call_args.kwargs["params"]["config"]
    return config


def test_train_sends_patience_on_supported_server() -> None:
    assert _train_config(_query_runner(_SUPPORTED), patience=2)["patience"] == 2


def test_train_omits_patience_when_unset() -> None:
    # The key only exists on GDS 2026.11+, so an unset patience must not reach any server.
    assert "patience" not in _train_config(_query_runner(_SUPPORTED), patience=None)
    assert "patience" not in _train_config(_query_runner(_UNSUPPORTED), patience=None)


def test_train_raises_when_patience_requested_on_unsupported_server() -> None:
    with pytest.raises(ValueError, match="requires GDS 2026.11"):
        _train_config(_query_runner(_UNSUPPORTED), patience=2)
