from typing import Any
from unittest import mock

import pandas as pd
import pytest

from graphdatascience.procedure_surface.cypher.node_embedding.fastrp_cypher_endpoints import FastRPCypherEndpoints
from graphdatascience.versions import ServerVersion

_SUPPORTED = ServerVersion(2026, 11, 0)
_UNSUPPORTED = ServerVersion(2025, 9, 0)


def _query_runner(server_version: ServerVersion) -> mock.Mock:
    query_runner = mock.Mock()
    query_runner.server_version.return_value = server_version
    query_runner.call_procedure.return_value.iloc = mock.MagicMock()
    query_runner.call_procedure.return_value.iloc.__getitem__.return_value = pd.Series(
        {
            "nodeCount": 4,
            "preProcessingMillis": 1,
            "computeMillis": 2,
            "mutateMillis": 3,
            "nodePropertiesWritten": 4,
            "configuration": {},
        }
    )
    return query_runner


def _mutate_config(query_runner: mock.Mock, result_as_vector: bool) -> dict[str, Any]:
    graph = mock.Mock()
    graph.name.return_value = "g"

    FastRPCypherEndpoints(query_runner).mutate(
        G=graph,
        mutate_property="embedding",
        embedding_dimension=8,
        result_as_vector=result_as_vector,
    )

    config: dict[str, Any] = query_runner.call_procedure.call_args.kwargs["params"]["config"]
    return config


def test_mutate_sends_result_as_vector_on_supported_server() -> None:
    assert _mutate_config(_query_runner(_SUPPORTED), result_as_vector=True)["resultAsVector"] is True
    assert _mutate_config(_query_runner(_SUPPORTED), result_as_vector=False)["resultAsVector"] is False


def test_mutate_omits_result_as_vector_on_unsupported_server() -> None:
    # The key only exists on GDS 2026.11+, so it must not reach older servers.
    assert "resultAsVector" not in _mutate_config(_query_runner(_UNSUPPORTED), result_as_vector=False)


def test_mutate_raises_when_result_as_vector_requested_on_unsupported_server() -> None:
    with pytest.raises(ValueError, match="requires GDS 2026.11"):
        _mutate_config(_query_runner(_UNSUPPORTED), result_as_vector=True)
