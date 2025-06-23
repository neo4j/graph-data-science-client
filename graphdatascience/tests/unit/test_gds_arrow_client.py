import json
import re
from typing import Any, Generator, Union

import pyarrow as pa
import pytest
from pyarrow._flight import GeneratorStream
from pyarrow.flight import (
    Action,
    FlightServerBase,
    FlightServerError,
    FlightTimedOutError,
    FlightUnavailableError,
    Ticket,
)

from graphdatascience.arrow_client.arrow_authentication import UsernamePasswordAuthentication
from graphdatascience.arrow_client.arrow_info import ArrowInfo
from graphdatascience.query_runner.gds_arrow_client import AuthMiddleware, GdsArrowClient

ActionParam = Union[str, tuple[str, Any], Action]


class FlightServer(FlightServerBase):  # type: ignore
    def __init__(self, location: str = "grpc://0.0.0.0:0", **kwargs: dict[str, Any]) -> None:
        super(FlightServer, self).__init__(location, **kwargs)
        self._location: str = location
        self._actions: list[ActionParam] = []
        self._tickets: list[Ticket] = []

    def do_get(self, context: Any, ticket: Ticket) -> GeneratorStream:
        self._tickets.append(ticket)
        table = pa.Table.from_pydict({"ids": [42, 1337, 1234]})
        return GeneratorStream(schema=table.schema, generator=table.to_batches())

    def do_action(self, context: Any, action: ActionParam) -> list[bytes]:
        self._actions.append(action)

        if isinstance(action, Action):
            actionType = action.type
        elif isinstance(action, tuple):
            actionType = action[0]
        elif isinstance(action, str):
            actionType = action

        response: dict[str, Any] = {}
        if "CREATE" in actionType:
            response = {"name": "g"}
        elif "NODE_LOAD_DONE" in actionType:
            response = {"name": "g", "node_count": 42}
        elif "RELATIONSHIP_LOAD_DONE" in actionType:
            response = {"name": "g", "relationship_count": 42}
        elif "TRIPLET_LOAD_DONE" in actionType:
            response = {"name": "g", "node_count": 42, "relationship_count": 1337}
        else:
            response = {}
        return [json.dumps(response).encode("utf-8")]


class FlakyFlightServer(FlightServerBase):  # type: ignore
    def __init__(self, location: str = "grpc://0.0.0.0:0", **kwargs: dict[str, Any]) -> None:
        super(FlakyFlightServer, self).__init__(location, **kwargs)
        self._location: str = location
        self._actions: list[ActionParam] = []
        self._tickets: list[Ticket] = []
        self._expected_failures = [
            FlightUnavailableError("Flight server is unavailable", "some reason"),
            FlightTimedOutError("Time out for some reason", "still timed out"),
        ]
        self._expected_retries = len(self._expected_failures) + 1

    def expected_retries(self) -> int:
        return self._expected_retries

    def do_get(self, context: Any, ticket: Ticket) -> GeneratorStream:
        self._tickets.append(ticket)

        if len(self._expected_failures) > 0:
            raise self._expected_failures.pop()

        table = pa.Table.from_pydict({"ids": [42, 1337, 1234]})
        return GeneratorStream(schema=table.schema, generator=table.to_batches())

    def do_action(self, context: Any, action: ActionParam) -> list[bytes]:
        self._actions.append(action)

        if len(self._expected_failures) > 0:
            raise self._expected_failures.pop()

        if isinstance(action, Action):
            actionType = action.type
        elif isinstance(action, tuple):
            actionType = action[0]
        elif isinstance(action, str):
            actionType = action

        response: dict[str, Any] = {}
        if "CREATE" in actionType:
            response = {"name": "g"}
        elif "NODE_LOAD_DONE" in actionType:
            response = {"name": "g", "node_count": 42}
        elif "RELATIONSHIP_LOAD_DONE" in actionType:
            response = {"name": "g", "relationship_count": 42}
        elif "TRIPLET_LOAD_DONE" in actionType:
            response = {"name": "g", "node_count": 42, "relationship_count": 1337}
        else:
            response = {}
        return [json.dumps(response).encode("utf-8")]


@pytest.fixture()
def flight_server() -> Generator[None, FlightServer, None]:
    with FlightServer() as server:
        yield server


@pytest.fixture()
def flaky_flight_server() -> Generator[None, FlakyFlightServer, None]:
    with FlakyFlightServer() as server:
        yield server


@pytest.fixture()
def flight_client(flight_server: FlightServer) -> Generator[GdsArrowClient, None, None]:
    with GdsArrowClient.create(ArrowInfo(f"localhost:{flight_server.port}", True, True, ["v1"])) as client:
        yield client


@pytest.fixture()
def flaky_flight_client(flaky_flight_server: FlakyFlightServer) -> Generator[GdsArrowClient, None, None]:
    with GdsArrowClient.create(ArrowInfo(f"localhost:{flaky_flight_server.port}", True, True, ["v1"])) as client:
        yield client


def test_create_graph_with_defaults(flight_server: FlightServer, flight_client: GdsArrowClient) -> None:
    flight_client.create_graph("g", "DB")
    actions = flight_server._actions
    assert len(actions) == 1
    assert_action(actions[0], "v1/CREATE_GRAPH", {"name": "g", "database_name": "DB"})


def test_create_graph_with_options(flight_server: FlightServer, flight_client: GdsArrowClient) -> None:
    flight_client.create_graph(
        "g", "DB", undirected_relationship_types=["Foo"], inverse_indexed_relationship_types=["Bar"], concurrency=42
    )
    actions = flight_server._actions
    assert len(actions) == 1
    assert_action(
        actions[0],
        "v1/CREATE_GRAPH",
        {
            "concurrency": 42,
            "database_name": "DB",
            "inverse_indexed_relationship_types": ["Bar"],
            "name": "g",
            "undirected_relationship_types": ["Foo"],
        },
    )


def test_create_graph_with_flaky_server(
    flaky_flight_server: FlakyFlightServer, flaky_flight_client: GdsArrowClient
) -> None:
    flaky_flight_client.create_graph("g", "DB")
    actions = flaky_flight_server._actions
    assert len(actions) == flaky_flight_server.expected_retries()
    assert_action(actions[0], "v1/CREATE_GRAPH", {"name": "g", "database_name": "DB"})


def test_create_graph_from_triplets_with_defaults(flight_server: FlightServer, flight_client: GdsArrowClient) -> None:
    flight_client.create_graph_from_triplets("g", "DB")
    actions = flight_server._actions
    assert len(actions) == 1
    assert_action(actions[0], "v1/CREATE_GRAPH_FROM_TRIPLETS", {"name": "g", "database_name": "DB"})


def test_create_graph_from_triplets_with_options(flight_server: FlightServer, flight_client: GdsArrowClient) -> None:
    flight_client.create_graph_from_triplets(
        "g", "DB", undirected_relationship_types=["Foo"], inverse_indexed_relationship_types=["Bar"], concurrency=42
    )
    actions = flight_server._actions
    assert len(actions) == 1
    assert_action(
        actions[0],
        "v1/CREATE_GRAPH_FROM_TRIPLETS",
        {
            "concurrency": 42,
            "database_name": "DB",
            "inverse_indexed_relationship_types": ["Bar"],
            "name": "g",
            "undirected_relationship_types": ["Foo"],
        },
    )


def test_create_database_with_defaults(flight_server: FlightServer, flight_client: GdsArrowClient) -> None:
    flight_client.create_database("g")
    actions = flight_server._actions
    assert len(actions) == 1
    assert_action(
        actions[0], "v1/CREATE_DATABASE", {"name": "g", "force": False, "high_io": False, "use_bad_collector": False}
    )


def test_create_database_with_options(flight_server: FlightServer, flight_client: GdsArrowClient) -> None:
    flight_client.create_database(
        "g",
        "DB",
        id_property="foo",
        db_format="BLOCK",
        concurrency=42,
        use_bad_collector=True,
        high_io=True,
        force=True,
    )
    actions = flight_server._actions
    assert len(actions) == 1
    assert_action(
        actions[0],
        "v1/CREATE_DATABASE",
        {
            "concurrency": 42,
            "db_format": "BLOCK",
            "force": True,
            "high_io": True,
            "id_property": "foo",
            "id_type": "DB",
            "name": "g",
            "use_bad_collector": True,
        },
    )


def test_node_load_done_action(flight_server: FlightServer, flight_client: GdsArrowClient) -> None:
    response = flight_client.node_load_done("g")
    assert response.name == "g"
    assert response.node_count == 42
    actions = flight_server._actions
    assert len(actions) == 1
    assert_action(actions[0], "v1/NODE_LOAD_DONE", {"name": "g"})


def test_relationship_load_done_action(flight_server: FlightServer, flight_client: GdsArrowClient) -> None:
    response = flight_client.relationship_load_done("g")
    assert response.name == "g"
    assert response.relationship_count == 42
    actions = flight_server._actions
    assert len(actions) == 1
    assert_action(actions[0], "v1/RELATIONSHIP_LOAD_DONE", {"name": "g"})


def test_triplet_load_done_action(flight_server: FlightServer, flight_client: GdsArrowClient) -> None:
    response = flight_client.triplet_load_done("g")
    assert response.name == "g"
    assert response.node_count == 42
    assert response.relationship_count == 1337
    actions = flight_server._actions
    assert len(actions) == 1
    assert_action(actions[0], "v1/TRIPLET_LOAD_DONE", {"name": "g"})


def test_abort_action(flight_server: FlightServer, flight_client: GdsArrowClient) -> None:
    flight_client.abort("g")
    actions = flight_server._actions
    assert len(actions) == 1
    assert_action(actions[0], "v1/ABORT", {"name": "g"})


def test_get_node_property(flight_server: FlightServer, flight_client: GdsArrowClient) -> None:
    flight_client.get_node_properties("g", "db", "id", ["Person"], concurrency=42)
    tickets = flight_server._tickets
    assert len(tickets) == 1
    assert_ticket(
        tickets[0],
        {
            "concurrency": 42,
            "configuration": {"list_node_labels": False, "node_labels": ["Person"], "node_property": "id"},
            "database_name": "db",
            "graph_name": "g",
            "procedure_name": "gds.graph.nodeProperty.stream",
        },
    )


def test_flakey_get_node_property(flaky_flight_server: FlakyFlightServer, flaky_flight_client: GdsArrowClient) -> None:
    flaky_flight_client.get_node_properties("g", "db", "id", ["Person"], concurrency=42)
    tickets = flaky_flight_server._tickets
    assert len(tickets) == flaky_flight_server.expected_retries()
    assert_ticket(
        tickets[0],
        {
            "concurrency": 42,
            "configuration": {"list_node_labels": False, "node_labels": ["Person"], "node_property": "id"},
            "database_name": "db",
            "graph_name": "g",
            "procedure_name": "gds.graph.nodeProperty.stream",
        },
    )


def test_get_node_properties(flight_server: FlightServer, flight_client: GdsArrowClient) -> None:
    flight_client.get_node_properties("g", "db", ["foo", "bar"], ["Person"], list_node_labels=True, concurrency=42)
    tickets = flight_server._tickets
    assert len(tickets) == 1
    assert_ticket(
        tickets[0],
        {
            "concurrency": 42,
            "configuration": {"list_node_labels": True, "node_labels": ["Person"], "node_properties": ["foo", "bar"]},
            "database_name": "db",
            "graph_name": "g",
            "procedure_name": "gds.graph.nodeProperties.stream",
        },
    )


def test_get_node_labels(flight_server: FlightServer, flight_client: GdsArrowClient) -> None:
    flight_client.get_node_labels("g", "db", concurrency=42)
    tickets = flight_server._tickets
    assert len(tickets) == 1
    assert_ticket(
        tickets[0],
        {
            "concurrency": 42,
            "configuration": {},
            "database_name": "db",
            "graph_name": "g",
            "procedure_name": "gds.graph.nodeLabels.stream",
        },
    )


def test_get_relationship_property(flight_server: FlightServer, flight_client: GdsArrowClient) -> None:
    flight_client.get_relationship_properties(
        "g", "db", relationship_properties="prop", relationship_types=["FOO"], concurrency=42
    )
    tickets = flight_server._tickets
    assert len(tickets) == 1
    assert_ticket(
        tickets[0],
        {
            "concurrency": 42,
            "configuration": {"relationship_property": "prop", "relationship_types": ["FOO"]},
            "database_name": "db",
            "graph_name": "g",
            "procedure_name": "gds.graph.relationshipProperty.stream",
        },
    )


def test_get_relationship_properties(flight_server: FlightServer, flight_client: GdsArrowClient) -> None:
    flight_client.get_relationship_properties(
        "g", "db", relationship_properties=["prop1", "prop2"], relationship_types=["FOO"], concurrency=42
    )
    tickets = flight_server._tickets
    assert len(tickets) == 1
    assert_ticket(
        tickets[0],
        {
            "concurrency": 42,
            "configuration": {"relationship_properties": ["prop1", "prop2"], "relationship_types": ["FOO"]},
            "database_name": "db",
            "graph_name": "g",
            "procedure_name": "gds.graph.relationshipProperties.stream",
        },
    )


def test_get_relationship_topologys(flight_server: FlightServer, flight_client: GdsArrowClient) -> None:
    flight_client.get_relationships("g", "db", relationship_types=["FOO"], concurrency=42)
    tickets = flight_server._tickets
    assert len(tickets) == 1
    assert_ticket(
        tickets[0],
        {
            "concurrency": 42,
            "configuration": {"relationship_types": ["FOO"]},
            "database_name": "db",
            "graph_name": "g",
            "procedure_name": "gds.graph.relationships.stream",
        },
    )


def test_auth_middleware() -> None:
    middleware = AuthMiddleware(UsernamePasswordAuthentication("user", "password"))

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
    middleware = AuthMiddleware(UsernamePasswordAuthentication("user", "password"))

    with pytest.raises(ValueError, match="Incompatible header value received from server: `12342`"):
        middleware.received_headers({"authorization": [12342]})


def test_handle_flight_error() -> None:
    with pytest.raises(
        FlightServerError,
        match="FlightServerError: UNKNOWN: Graph with name `people-and-fruits` does not exist on database `neo4j`. It might exist on another database.",
    ):
        GdsArrowClient.handle_flight_error(
            FlightServerError(
                'FlightServerError: Flight RPC failed with message: org.apache.arrow.flight.FlightRuntimeException: UNKNOWN: Graph with name `people-and-fruits` does not exist on database `neo4j`. It might exist on another database.. gRPC client debug context: UNKNOWN:Error received from peer ipv4:35.241.177.75:8491 {created_time:"2024-08-29T15:59:03.828903999+02:00", grpc_status:2, grpc_message:"org.apache.arrow.flight.FlightRuntimeException: UNKNOWN: Graph with name `people-and-fruits` does not exist on database `neo4j`. It might exist on another database."}. Client context: IOError: Server never sent a data message. Detail: Internal'
            )
        )

    with pytest.raises(
        FlightServerError,
        match=re.escape("FlightServerError: UNKNOWN: Unexpected configuration key(s): [undirectedRelationshipTypes]"),
    ):
        GdsArrowClient.handle_flight_error(
            FlightServerError(
                "FlightServerError: Flight returned internal error, with message: org.apache.arrow.flight.FlightRuntimeException: UNKNOWN: Unexpected configuration key(s): [undirectedRelationshipTypes]"
            )
        )


def assert_action(action: Action, expected_type: str, expected_body: dict[str, Any]) -> None:
    assert action.type == expected_type
    assert json.loads(action.body.to_pybytes().decode()) == expected_body


def assert_ticket(ticket: Ticket, expected_body: dict[str, Any]) -> None:
    parsed = json.loads(ticket.ticket.decode())
    assert parsed["name"] == "GET_COMMAND"
    assert parsed["body"] == expected_body
