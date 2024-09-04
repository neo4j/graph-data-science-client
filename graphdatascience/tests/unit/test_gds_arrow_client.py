import re

import pytest
from pyarrow import flight

from graphdatascience.query_runner.gds_arrow_client import AuthMiddleware, GdsArrowClient


def test_auth_middleware() -> None:
    middleware = AuthMiddleware(("user", "password"))

    first_header = middleware.sending_headers()
    assert first_header == {"authorization": "Basic dXNlcjpwYXNzd29yZA=="}

    middleware.received_headers({"authorization": ["Bearer token"]})
    assert middleware._token == "token"

    second_header = middleware.sending_headers()
    assert second_header == {"authorization": "Bearer token"}

    middleware.received_headers({})
    assert middleware._token == "token"

    second_header = middleware.sending_headers()
    assert second_header == {"authorization": "Bearer token"}


def test_auth_middleware_bad_headers() -> None:
    middleware = AuthMiddleware(("user", "password"))

    with pytest.raises(ValueError, match="Incompatible header value received from server: `12342`"):
        middleware.received_headers({"authorization": [12342]})


def test_handle_flight_error():
    with pytest.raises(
        flight.FlightServerError,
        match="FlightServerError: UNKNOWN: Graph with name `people-and-fruits` does not exist on database `neo4j`. It might exist on another database.",
    ):
        GdsArrowClient.handle_flight_error(
            flight.FlightServerError(
                'FlightServerError: Flight RPC failed with message: org.apache.arrow.flight.FlightRuntimeException: UNKNOWN: Graph with name `people-and-fruits` does not exist on database `neo4j`. It might exist on another database.. gRPC client debug context: UNKNOWN:Error received from peer ipv4:35.241.177.75:8491 {created_time:"2024-08-29T15:59:03.828903999+02:00", grpc_status:2, grpc_message:"org.apache.arrow.flight.FlightRuntimeException: UNKNOWN: Graph with name `people-and-fruits` does not exist on database `neo4j`. It might exist on another database."}. Client context: IOError: Server never sent a data message. Detail: Internal'
            )
        )

    with pytest.raises(
        flight.FlightServerError,
        match=re.escape("FlightServerError: UNKNOWN: Unexpected configuration key(s): [undirectedRelationshipTypes]"),
    ):
        GdsArrowClient.handle_flight_error(
            flight.FlightServerError(
                "FlightServerError: Flight returned internal error, with message: org.apache.arrow.flight.FlightRuntimeException: UNKNOWN: Unexpected configuration key(s): [undirectedRelationshipTypes]"
            )
        )
