import json
import re
from typing import Any, Dict

import pyarrow as pa
import pytest
from pyarrow import flight
from pyarrow._flight import GeneratorStream

from graphdatascience.query_runner.gds_arrow_client import AuthMiddleware, GdsArrowClient


class FlightServer(flight.FlightServerBase):
    def __init__(self, location="grpc://0.0.0.0:0", **kwargs):
        super(FlightServer, self).__init__(location, **kwargs)
        self._location = location
        self._actions = []
        self._tickets = []

    def do_get(self, context, ticket):
        self._tickets.append(ticket)
        table = pa.Table.from_pydict({"ids": [42, 1337, 1234]})
        return GeneratorStream(schema=table.schema, generator=table.to_batches())

    def do_action(self, context, action):
        self._actions.append(action)
        if "CREATE" in action.type:
            response = {"name": "g"}
        elif "NODE_LOAD_DONE" in action.type:
            response = {"name": "g", "node_count": 42}
        elif "RELATIONSHIP_LOAD_DONE" in action.type:
            response = {"name": "g", "relationship_count": 42}
        elif "TRIPLET_LOAD_DONE" in action.type:
            response = {"name": "g", "node_count": 42, "relationship_count": 1337}
        else:
            response = {}
        return [json.dumps(response).encode("utf-8")]


@pytest.fixture()
def flight_server():
    with FlightServer() as server:
        yield server


@pytest.fixture()
def flight_client(flight_server):
    with GdsArrowClient("localhost", flight_server.port) as client:
        yield client


def test_create_graph_with_defaults(flight_server, flight_client):
    flight_client.create_graph("g", "DB")
    actions = flight_server._actions
    assert len(actions) == 1
    assert_action(actions[0], "v1/CREATE_GRAPH", {"name": "g", "database_name": "DB"})


def test_create_graph_with_options(flight_server, flight_client):
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


def test_create_graph_from_triplets_with_defaults(flight_server, flight_client):
    flight_client.create_graph_from_triplets("g", "DB")
    actions = flight_server._actions
    assert len(actions) == 1
    assert_action(actions[0], "v1/CREATE_GRAPH_FROM_TRIPLETS", {"name": "g", "database_name": "DB"})


def test_create_graph_from_triplets_with_options(flight_server, flight_client):
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


def test_create_database_with_defaults(flight_server, flight_client):
    flight_client.create_database("g")
    actions = flight_server._actions
    assert len(actions) == 1
    assert_action(
        actions[0], "v1/CREATE_DATABASE", {"name": "g", "force": False, "high_io": False, "use_bad_collector": False}
    )


def test_create_database_with_options(flight_server, flight_client):
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


def test_node_load_done_action(flight_server, flight_client):
    response = flight_client.node_load_done("g")
    assert response.name == "g"
    assert response.node_count == 42
    actions = flight_server._actions
    assert len(actions) == 1
    assert_action(actions[0], "v1/NODE_LOAD_DONE", {"name": "g"})


def test_relationship_load_done_action(flight_server, flight_client):
    response = flight_client.relationship_load_done("g")
    assert response.name == "g"
    assert response.relationship_count == 42
    actions = flight_server._actions
    assert len(actions) == 1
    assert_action(actions[0], "v1/RELATIONSHIP_LOAD_DONE", {"name": "g"})


def test_triplet_load_done_action(flight_server, flight_client):
    response = flight_client.triplet_load_done("g")
    assert response.name == "g"
    assert response.node_count == 42
    assert response.relationship_count == 1337
    actions = flight_server._actions
    assert len(actions) == 1
    assert_action(actions[0], "v1/TRIPLET_LOAD_DONE", {"name": "g"})


def test_abort_action(flight_server, flight_client):
    flight_client.abort("g")
    actions = flight_server._actions
    assert len(actions) == 1
    assert_action(actions[0], "v1/ABORT", {"name": "g"})


def test_get_node_property(flight_server, flight_client):
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


def test_get_node_properties(flight_server, flight_client):
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


def test_get_node_labels(flight_server, flight_client):
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


def test_get_relationship_property(flight_server, flight_client):
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


def test_get_relationship_properties(flight_server, flight_client):
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


def test_get_relationship_topologys(flight_server, flight_client):
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


def assert_action(action, expected_type: str, expected_body: Dict[str, Any]):
    assert action.type == expected_type
    assert json.loads(action.body.to_pybytes().decode()) == expected_body


def assert_ticket(ticket, expected_body: Dict[str, Any]):
    parsed = json.loads(ticket.ticket.decode())
    assert parsed["name"] == "GET_COMMAND"
    assert parsed["body"] == expected_body
