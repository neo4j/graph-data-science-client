from typing import Any

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient


def build_arrow_config(arrow_client: AuthenticatedArrowClient, batch_size: int | None = None) -> dict[str, Any]:
    connection_info = arrow_client.advertised_connection_info()

    token = arrow_client.request_token()
    if token is None:
        token = "IGNORED"

    config: dict[str, Any] = {
        "host": connection_info.host,
        "port": connection_info.port,
        "token": token,
        "encrypted": connection_info.encrypted,
    }

    if batch_size is not None:
        config["batchSize"] = batch_size

    return config
