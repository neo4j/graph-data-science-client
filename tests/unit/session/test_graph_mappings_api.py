from typing import cast

from requests_mock import Mocker
from requests_mock.request import _RequestObjectProxy

from graphdatascience.session.aura_api import AuraApi
from graphdatascience.session.aura_api_responses import GraphMapping
from graphdatascience.session.graph_mappings_api import GraphMappings

MAPPING_JSON = {
    "graph_name": "g",
    "database_username": "neo4j",
    "database_uuid": "00000000-0000-0000-0000-000000000021",
    "session_id": "session-1",
    "linked": False,
    "created_at": "2020-01-01T00:00:00.000Z",
}


def _api() -> AuraApi:
    return AuraApi(client_id="", client_secret="", project_id="some-tenant")


def _graph_mappings() -> GraphMappings:
    return GraphMappings(_api())


def test_create_graph_mapping(requests_mock: Mocker) -> None:
    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "token", "expires_in": 500, "token_type": "Bearer"},
    )
    requests_mock.post(
        "https://api.neo4j.io/v1/graph-analytics/sessions/session-1/graphs",
        json={"data": MAPPING_JSON, "errors": []},
    )

    mapping = _graph_mappings().create(
        session_id="session-1", graph_name="g", database_username="neo4j", database_uuid="db-uuid"
    )

    request = cast(_RequestObjectProxy, requests_mock.request_history[-1])
    assert request.method == "POST"
    assert request.json() == {
        "graph_name": "g",
        "database_username": "neo4j",
        "database_uuid": "db-uuid",
        "linked": False,
    }
    assert mapping == GraphMapping.from_json(MAPPING_JSON)


def test_list_graph_mappings(requests_mock: Mocker) -> None:
    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "token", "expires_in": 500, "token_type": "Bearer"},
    )
    requests_mock.get(
        "https://api.neo4j.io/v1/graph-analytics/sessions/graphs",
        json={"data": [MAPPING_JSON], "errors": []},
    )

    mappings = _graph_mappings().list(database_username="neo4j", database_uuid="db-uuid")

    request = cast(_RequestObjectProxy, requests_mock.request_history[-1])
    assert request.method == "GET"
    assert request.qs == {"databaseusername": ["neo4j"], "databaseuuid": ["db-uuid"]}
    assert mappings == [GraphMapping.from_json(MAPPING_JSON)]


def test_list_graph_mappings_with_graph_name_filter(requests_mock: Mocker) -> None:
    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "token", "expires_in": 500, "token_type": "Bearer"},
    )
    requests_mock.get(
        "https://api.neo4j.io/v1/graph-analytics/sessions/graphs",
        json={"data": [], "errors": []},
    )

    _graph_mappings().list(database_username="neo4j", database_uuid="db-uuid", graph_name="g")

    request = cast(_RequestObjectProxy, requests_mock.request_history[-1])
    assert request.qs == {"databaseusername": ["neo4j"], "databaseuuid": ["db-uuid"], "graphname": ["g"]}


def test_delete_graph_mapping(requests_mock: Mocker) -> None:
    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "token", "expires_in": 500, "token_type": "Bearer"},
    )
    requests_mock.delete(
        "https://api.neo4j.io/v1/graph-analytics/sessions/graphs",
        json={"data": {}, "errors": []},
    )

    deleted = _graph_mappings().delete(graph_name="g", database_username="neo4j", database_uuid="db-uuid")

    request = cast(_RequestObjectProxy, requests_mock.request_history[-1])
    assert request.method == "DELETE"
    assert request.json() == {
        "graph_name": "g",
        "database_username": "neo4j",
        "database_uuid": "db-uuid",
    }
    assert deleted


def test_delete_missing_graph_mapping_returns_false(requests_mock: Mocker) -> None:
    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "token", "expires_in": 500, "token_type": "Bearer"},
    )
    requests_mock.delete(
        "https://api.neo4j.io/v1/graph-analytics/sessions/graphs",
        status_code=404,
        json={},
    )

    deleted = _graph_mappings().delete(graph_name="g", database_username="neo4j", database_uuid="db-uuid")
    assert not deleted


def test_create_graph_mapping_with_errors(requests_mock: Mocker) -> None:
    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "token", "expires_in": 500, "token_type": "Bearer"},
    )
    requests_mock.post(
        "https://api.neo4j.io/v1/graph-analytics/sessions/session-1/graphs",
        json={"data": None, "errors": [{"id": "session-1", "reason": "some reason", "message": "some message"}]},
    )

    try:
        _graph_mappings().create(
            session_id="session-1", graph_name="g", database_username="neo4j", database_uuid="db-uuid"
        )
        raise AssertionError("expected a SessionStatusError")
    except Exception as e:
        assert "some message" in str(e)


def test_register_is_best_effort(mocker) -> None:
    from graphdatascience.session.graph_mapping_context import GraphMappingContext

    graph_mappings = mocker.Mock()
    graph_mappings.create.side_effect = RuntimeError("boom")

    context = GraphMappingContext(
        session_id="session-1",
        database_username="neo4j",
        database_uuid="db-uuid",
        graph_mappings=graph_mappings,
    )

    # must not raise, the created graph stays usable
    context.register("g")

    graph_mappings.create.assert_called_once_with("session-1", "g", "neo4j", "db-uuid")


def test_unregister_is_best_effort(mocker) -> None:
    from graphdatascience.session.graph_mapping_context import GraphMappingContext

    graph_mappings = mocker.Mock()
    graph_mappings.delete.side_effect = RuntimeError("boom")

    context = GraphMappingContext(
        session_id="session-1",
        database_username="neo4j",
        database_uuid="db-uuid",
        graph_mappings=graph_mappings,
    )

    # must not raise, the dropped graph stays dropped
    context.unregister("g")
