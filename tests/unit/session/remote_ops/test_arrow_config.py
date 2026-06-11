from unittest.mock import MagicMock

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient, ConnectionInfo
from graphdatascience.session.remote_ops.arrow_config import build_arrow_config


def test_arrow_config_falls_back_to_ignored_token_when_none() -> None:
    arrow_client = MagicMock(spec=AuthenticatedArrowClient)
    arrow_client.advertised_connection_info.return_value = ConnectionInfo(host="arrow.host", port=1234, encrypted=False)
    arrow_client.request_token.return_value = None

    config = build_arrow_config(arrow_client, None)

    assert "batchSize" not in config
    assert config["token"] == "IGNORED"
    assert config["encrypted"] is False
