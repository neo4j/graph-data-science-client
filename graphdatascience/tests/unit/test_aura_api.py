import logging
import re
from datetime import datetime, timedelta, timezone

import pytest
from _pytest.logging import LogCaptureFixture
from requests_mock import Mocker
from requests_mock.request import _RequestObjectProxy

from graphdatascience.session import SessionMemory
from graphdatascience.session.algorithm_category import AlgorithmCategory
from graphdatascience.session.aura_api import AuraApi, AuraApiError, SessionStatusError
from graphdatascience.session.aura_api_responses import (
    EstimationDetails,
    InstanceCreateDetails,
    InstanceSpecificDetails,
    ProjectDetails,
    SessionDetails,
    SessionDetailsWithErrors,
    SessionErrorData,
    TimeParser,
    WaitResult,
)
from graphdatascience.session.cloud_location import CloudLocation
from graphdatascience.session.session_sizes import SESSION_MEMORY_VALUE_UNKNOWN


def mock_auth_token(requests_mock: Mocker) -> None:
    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "very_short_token", "expires_in": 500, "token_type": "Bearer"},
    )


def test_base_uri_from_env() -> None:
    assert AuraApi.base_uri("dev") == "https://api-dev.neo4j-dev.io"
    assert AuraApi.base_uri(None) == "https://api.neo4j.io"
    assert AuraApi.base_uri("staging") == "https://api-staging.neo4j.io"


def test_create_attached_session(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", project_id="some-tenant")

    mock_auth_token(requests_mock)

    def assert_body(request: _RequestObjectProxy) -> bool:
        assert request.json() == {
            "name": "name-0",
            "memory": "4GB",
            "instance_id": "dbid-1",
            "ttl": "42.0s",
            "project_id": "some-tenant",
        }
        return True

    requests_mock.post(
        "https://api.neo4j.io/v1/graph-analytics/sessions",
        json={
            "data": {
                "id": "id0",
                "name": "name-0",
                "status": "Creating",
                "instance_id": "dbid-1",
                "created_at": "1970-01-01T00:00:00Z",
                "host": "1.2.3.4",
                "memory": "4Gi",
                "project_id": "some-tenant",
                "user_id": "user-0",
                "ttl": "42s",
            }
        },
        additional_matcher=assert_body,
    )

    result = api.get_or_create_session(
        name="name-0", dbid="dbid-1", memory=SessionMemory.m_4GB.value, ttl=timedelta(seconds=42)
    )

    assert result == SessionDetails(
        id="id0",
        name="name-0",
        status="Creating",
        instance_id="dbid-1",
        created_at=TimeParser.fromisoformat("1970-01-01T00:00:00Z"),
        host="1.2.3.4",
        memory=SessionMemory.m_4GB.value,
        expiry_date=None,
        ttl=timedelta(seconds=42),
        project_id="some-tenant",
        user_id="user-0",
    )


def test_create_dedicated_session(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", project_id="some-tenant")

    mock_auth_token(requests_mock)

    def assert_body(request: _RequestObjectProxy) -> bool:
        assert request.json() == {
            "name": "name-0",
            "project_id": "some-tenant",
            "memory": "4GB",
            "cloud_provider": "aws",
            "region": "leipzig-1",
            "ttl": "42.0s",
        }
        return True

    requests_mock.post(
        "https://api.neo4j.io/v1/graph-analytics/sessions",
        json={
            "data": {
                "id": "id0",
                "name": "name-0",
                "status": "Creating",
                "instance_id": "",
                "created_at": "1970-01-01T00:00:00Z",
                "host": "1.2.3.4",
                "memory": "4Gi",
                "project_id": "tenant-0",
                "user_id": "user-0",
                "ttl": "42.0s",
            }
        },
        additional_matcher=assert_body,
    )

    result = api.get_or_create_session(
        "name-0",
        SessionMemory.m_4GB.value,
        ttl=timedelta(seconds=42),
        cloud_location=CloudLocation(
            "aws",
            "leipzig-1",
        ),
    )
    assert result == SessionDetails(
        id="id0",
        name="name-0",
        status="Creating",
        instance_id=None,
        created_at=TimeParser.fromisoformat("1970-01-01T00:00:00Z"),
        host="1.2.3.4",
        memory=SessionMemory.m_4GB.value,
        expiry_date=None,
        ttl=timedelta(seconds=42),
        project_id="tenant-0",
        user_id="user-0",
    )


def test_create_standalone_session_http_error_forwards(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", project_id="some-tenant")

    mock_auth_token(requests_mock)

    requests_mock.post(
        "https://api.neo4j.io/v1/graph-analytics/sessions",
        status_code=400,
        json={
            "errors": {
                "message": "some validation error",
            }
        },
    )

    with pytest.raises(AuraApiError, match="some validation error"):
        api.get_or_create_session(
            "name-0",
            SessionMemory.m_4GB.value,
            ttl=timedelta(seconds=42),
            cloud_location=CloudLocation("invalidProvider", "leipzig-1"),
        )


def test_create_standalone_session_state_error_forwards(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", project_id="some-tenant")

    mock_auth_token(requests_mock)

    requests_mock.post(
        "https://api.neo4j.io/v1/graph-analytics/sessions",
        status_code=200,
        json={
            "data": {
                "id": "id0",
                "name": "name-0",
                "status": "Failed",
                "instance_id": "",
                "created_at": "1970-01-01T00:00:00Z",
                "host": "1.2.3.4",
                "memory": "4Gi",
                "project_id": "tenant-0",
                "user_id": "user-0",
                "ttl": "42.0s",
            },
            "errors": [
                {
                    "id": "id0",
                    "message": "details here",
                    "reason": "OutOfMemory",
                }
            ],
        },
    )

    with pytest.raises(
        SessionStatusError,
        match=re.escape("Session is in an unhealthy state. Details: ['Reason: OutOfMemory, Message: details here']"),
    ):
        api.get_or_create_session(
            "name-0",
            SessionMemory.m_4GB.value,
            ttl=timedelta(seconds=42),
            cloud_location=CloudLocation("gcp", "leipzig-1"),
        )


def test_get_session(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", project_id="some-tenant")

    mock_auth_token(requests_mock)

    requests_mock.get(
        "https://api.neo4j.io/v1/graph-analytics/sessions/id0",
        json={
            "data": {
                "id": "id0",
                "name": "name-0",
                "status": "Ready",
                "instance_id": "dbid-1",
                "created_at": "1970-01-01T00:00:00Z",
                "host": "1.2.3.4",
                "memory": "4Gi",
                "expiry_date": "1977-01-01T00:00:00Z",
                "project_id": "tenant-0",
                "user_id": "user-0",
            }
        },
    )

    result = api.get_session("id0")

    assert result == SessionDetails(
        id="id0",
        name="name-0",
        status="Ready",
        instance_id="dbid-1",
        created_at=TimeParser.fromisoformat("1970-01-01T00:00:00Z"),
        host="1.2.3.4",
        memory=SessionMemory.m_4GB.value,
        expiry_date=TimeParser.fromisoformat("1977-01-01T00:00:00Z"),
        ttl=None,
        project_id="tenant-0",
        user_id="user-0",
    )


def test_get_session_state_error_forwards(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", project_id="some-tenant")

    mock_auth_token(requests_mock)

    requests_mock.get(
        "https://api.neo4j.io/v1/graph-analytics/sessions/id0",
        json={
            "data": {
                "id": "id0",
                "name": "name-0",
                "status": "Failed",
                "instance_id": "dbid-1",
                "created_at": "1970-01-01T00:00:00Z",
                "host": "1.2.3.4",
                "memory": "4Gi",
                "expiry_date": "1977-01-01T00:00:00Z",
                "project_id": "tenant-0",
                "user_id": "user-0",
                "ttl": "42s",
            },
            "errors": [
                {
                    "id": "id0",
                    "message": "details here",
                    "reason": "OutOfMemory",
                }
            ],
        },
    )

    with pytest.raises(
        SessionStatusError,
        match=re.escape("Session is in an unhealthy state. Details: ['Reason: OutOfMemory, Message: details here']"),
    ):
        api.get_session("id0")


def test_list_sessions(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", project_id="some-tenant")
    mock_auth_token(requests_mock)

    requests_mock.get(
        "https://api.neo4j.io/v1/graph-analytics/sessions?projectId=some-tenant",
        json={
            "data": [
                {
                    "id": "id0",
                    "name": "name-0",
                    "status": "Ready",
                    "instance_id": "dbid-1",
                    "created_at": "1970-01-01T00:00:00Z",
                    "host": "1.2.3.4",
                    "memory": "4Gi",
                    "expiry_date": "1977-01-01T00:00:00Z",
                    "project_id": "tenant-1",
                    "user_id": "user-1",
                    "cloud_provider": "gcp",
                    "region": "leipzig",
                },
                {
                    "id": "id1",
                    "name": "name-2",
                    "status": "Creating",
                    "instance_id": "dbid-3",
                    "created_at": "2012-01-01T00:00:00Z",
                    "memory": "8Gi",
                    "host": "foo.bar",
                    "project_id": "tenant-2",
                    "user_id": "user-2",
                },
            ]
        },
    )

    result = api.list_sessions()

    expected1 = SessionDetailsWithErrors(
        id="id0",
        name="name-0",
        status="Ready",
        instance_id="dbid-1",
        created_at=TimeParser.fromisoformat("1970-01-01T00:00:00Z"),
        host="1.2.3.4",
        memory=SessionMemory.m_4GB.value,
        expiry_date=TimeParser.fromisoformat("1977-01-01T00:00:00Z"),
        ttl=None,
        project_id="tenant-1",
        user_id="user-1",
        cloud_location=CloudLocation("gcp", "leipzig"),
    )

    expected2 = SessionDetailsWithErrors(
        id="id1",
        name="name-2",
        status="Creating",
        instance_id="dbid-3",
        created_at=TimeParser.fromisoformat("2012-01-01T00:00:00Z"),
        memory=SessionMemory.m_8GB.value,
        host="foo.bar",
        expiry_date=None,
        ttl=None,
        project_id="tenant-2",
        user_id="user-2",
        cloud_location=None,
    )

    assert result == [expected1, expected2]


def test_list_sessions_with_db_id(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", project_id="some-tenant")
    mock_auth_token(requests_mock)

    requests_mock.get(
        "https://api.neo4j.io/v1/graph-analytics/sessions?projectId=some-tenant&instanceId=dbid",
        json={
            "data": [
                {
                    "id": "id0",
                    "name": "name-0",
                    "status": "Ready",
                    "instance_id": "dbid-1",
                    "created_at": "1970-01-01T00:00:00Z",
                    "host": "1.2.3.4",
                    "memory": "4Gi",
                    "expiry_date": "1977-01-01T00:00:00Z",
                    "project_id": "tenant-1",
                    "user_id": "user-1",
                },
                {
                    "id": "id1",
                    "name": "name-2",
                    "status": "Creating",
                    "instance_id": "dbid-3",
                    "created_at": "2012-01-01T00:00:00Z",
                    "memory": "8Gi",
                    "host": "foo.bar",
                    "project_id": "tenant-2",
                    "user_id": "user-2",
                },
            ]
        },
    )

    result = api.list_sessions("dbid")

    expected1 = SessionDetailsWithErrors(
        id="id0",
        name="name-0",
        status="Ready",
        instance_id="dbid-1",
        created_at=TimeParser.fromisoformat("1970-01-01T00:00:00Z"),
        host="1.2.3.4",
        memory=SessionMemory.m_4GB.value,
        expiry_date=TimeParser.fromisoformat("1977-01-01T00:00:00Z"),
        ttl=None,
        project_id="tenant-1",
        user_id="user-1",
    )

    expected2 = SessionDetailsWithErrors(
        id="id1",
        name="name-2",
        status="Creating",
        instance_id="dbid-3",
        created_at=TimeParser.fromisoformat("2012-01-01T00:00:00Z"),
        memory=SessionMemory.m_8GB.value,
        host="foo.bar",
        expiry_date=None,
        ttl=None,
        project_id="tenant-2",
        user_id="user-2",
    )

    assert result == [expected1, expected2]


def test_list_session_state_error_forwards(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", project_id="some-tenant")

    mock_auth_token(requests_mock)

    requests_mock.get(
        "https://api.neo4j.io/v1/graph-analytics/sessions",
        json={
            "data": [
                {
                    "id": "id0",
                    "name": "name-0",
                    "status": "Ready",
                    "instance_id": "dbid-1",
                    "created_at": "1970-01-01T00:00:00Z",
                    "host": "1.2.3.4",
                    "memory": "4Gi",
                    "expiry_date": "1977-01-01T00:00:00Z",
                    "project_id": "tenant-1",
                    "user_id": "user-1",
                },
                {
                    "id": "id1",
                    "name": "name-2",
                    "status": "Creating",
                    "instance_id": "dbid-3",
                    "created_at": "2012-01-01T00:00:00Z",
                    "memory": "8Gi",
                    "host": "foo.bar",
                    "project_id": "tenant-2",
                    "user_id": "user-2",
                },
                {
                    "id": "id3",
                    "name": "name-3",
                    "status": "Creating",
                    "instance_id": "dbid-3",
                    "created_at": "2012-01-01T00:00:00Z",
                    "memory": "8Gi",
                    "host": "foo.bar",
                    "project_id": "tenant-2",
                    "user_id": "user-2",
                },
            ],
            "errors": [
                {
                    "id": "id0",
                    "message": "Session reached its memory limit. Create a larger instance.",
                    "reason": "OutOfMemory",
                },
                {
                    "id": "id1",
                    "message": "Error 1.",
                    "reason": "Reason1",
                },
                {
                    "id": "id1",
                    "message": "Error 2.",
                    "reason": "Reason2",
                },
            ],
        },
    )

    sessions = api.list_sessions()
    errors = {s.id: s.errors for s in sessions}

    assert errors["id0"] == [
        SessionErrorData(
            message="Session reached its memory limit. Create a larger instance.",
            reason="OutOfMemory",
        )
    ]

    assert errors["id1"] == [
        SessionErrorData(message="Error 1.", reason="Reason1"),
        SessionErrorData(message="Error 2.", reason="Reason2"),
    ]

    assert errors["id3"] is None


def test_delete_session(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", project_id="some-tenant")

    mock_auth_token(requests_mock)
    requests_mock.delete(
        "https://api.neo4j.io/v1/graph-analytics/sessions/id0",
        status_code=202,
    )

    assert api.delete_session("id0") is True


def test_delete_missing_session(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", project_id="some-tenant")

    mock_auth_token(requests_mock)
    requests_mock.delete(
        "https://api.neo4j.io/v1/graph-analytics/sessions/id0",
        status_code=404,
    )

    assert api.delete_session("id0") is False


def test_multiple_tenants(requests_mock: Mocker) -> None:
    mock_auth_token(requests_mock)

    requests_mock.get(
        "https://api.neo4j.io/v1/tenants",
        json={
            "data": [
                {"id": "tenant1", "name": "Production"},
                {"id": "tenant2", "name": "Development"},
            ]
        },
    )

    with pytest.raises(
        RuntimeError,
        match="This account has access to multiple projects: `{'tenant1': 'Production', 'tenant2': 'Development'}`",
    ):
        AuraApi(client_id="", client_secret="")


def test_dont_wait_forever_for_session(requests_mock: Mocker, caplog: LogCaptureFixture) -> None:
    mock_auth_token(requests_mock)
    requests_mock.get(
        "https://api.neo4j.io/v1/graph-analytics/sessions/id0",
        json={
            "data": {
                "id": "id0",
                "name": "name-0",
                "status": "Creating",
                "instance_id": "dbid-1",
                "created_at": "1970-01-01T00:00:00Z",
                "host": "foo.bar",
                "memory": "4Gi",
                "expiry_date": "1977-01-01T00:00:00Z",
                "project_id": "tenant-1",
                "user_id": "user-1",
            }
        },
    )

    api = AuraApi("", "", project_id="some-tenant")

    with caplog.at_level(logging.DEBUG):
        assert (
            "Session `id0` is not running after 0.2 seconds"
            in api.wait_for_session_running("id0", sleep_time=0.05, max_wait_time=0.2).error
        )

    assert "Session `id0` is not yet running. Current status: Creating Host: foo.bar. Retrying in 0.1" in caplog.text


def test_wait_for_session_running(requests_mock: Mocker) -> None:
    mock_auth_token(requests_mock)
    requests_mock.get(
        "https://api.neo4j.io/v1/graph-analytics/sessions/id0",
        json={
            "data": {
                "id": "id0",
                "name": "name-0",
                "status": "Ready",
                "instance_id": "dbid-1",
                "created_at": "1970-01-01T00:00:00Z",
                "host": "foo.bar",
                "memory": "4Gi",
                "expiry_date": "1977-01-01T00:00:00Z",
                "project_id": "tenant-1",
                "user_id": "user-1",
            }
        },
    )

    api = AuraApi("", "", project_id="some-tenant")

    assert api.wait_for_session_running("id0") == WaitResult.from_connection_url("neo4j+s://foo.bar")


def test_wait_for_session_running_until_failure(requests_mock: Mocker) -> None:
    mock_auth_token(requests_mock)
    requests_mock.get(
        "https://api.neo4j.io/v1/graph-analytics/sessions/id0",
        json={
            "data": {
                "id": "id0",
                "name": "name-0",
                "status": "Failed",
                "instance_id": "dbid-1",
                "created_at": "1970-01-01T00:00:00Z",
                "host": "foo.bar",
                "memory": "4Gi",
                "expiry_date": "1977-01-01T00:00:00Z",
                "project_id": "tenant-1",
                "user_id": "user-1",
            },
            "errors": [
                {
                    "id": "id0",
                    "message": "Session oomed...",
                    "reason": "OOM",
                },
            ],
        },
    )

    api = AuraApi("", "", project_id="some-tenant")

    with pytest.raises(SessionStatusError, match="Session is in an unhealthy state"):
        api.wait_for_session_running("id0")


def test_wait_for_session_running_until_expired(requests_mock: Mocker) -> None:
    mock_auth_token(requests_mock)
    requests_mock.get(
        "https://api.neo4j.io/v1/graph-analytics/sessions/id0",
        json={
            "data": {
                "id": "id0",
                "name": "name-0",
                "status": "Expired",
                "instance_id": "dbid-1",
                "created_at": "1970-01-01T00:00:00Z",
                "host": "foo.bar",
                "memory": "4Gi",
                "expiry_date": "1977-01-01T00:00:00Z",
                "project_id": "tenant-1",
                "user_id": "user-1",
            },
            "errors": [{"id": "id0", "message": "Session is expired", "reason": "Inactivity"}],
        },
    )

    api = AuraApi("", "", project_id="some-tenant")

    with pytest.raises(SessionStatusError, match="Session is in an unhealthy state"):
        api.wait_for_session_running("id0")


def test_delete_instance(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", project_id="some-tenant")

    mock_auth_token(requests_mock)

    requests_mock.delete(
        "https://api.neo4j.io/v1/instances/id0",
        status_code=202,
        json={
            "data": {
                "id": "id0",
                "name": "",
                "status": "deleting",
                "connection_url": "",
                "tenant_id": "",
                "cloud_provider": "",
                "memory": "4Gi",
                "region": "",
                "type": "",
            }
        },
    )

    result = api.delete_instance("id0")

    assert result == InstanceSpecificDetails(
        id="id0",
        name="",
        project_id="",
        cloud_provider="",
        status="deleting",
        connection_url="",
        memory=SessionMemory.m_4GB.value,
        region="",
        type="",
    )


def test_delete_already_deleting_instance(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", project_id="some-tenant")

    mock_auth_token(requests_mock)
    requests_mock.delete(
        "https://api.neo4j.io/v1/instances/id0",
        status_code=404,
        reason="Not Found",
        json={"errors": [{"message": "DB not found: id0", "reason": "db-not-found"}]},
    )

    result = api.delete_instance("id0")
    assert result is None


def test_delete_that_fails(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", project_id="some-tenant")

    mock_auth_token(requests_mock)
    requests_mock.delete(
        "https://api.neo4j.io/v1/instances/id0",
        status_code=500,
        reason="Internal Server Error",
        json={"errors": [{"message": "some failure happened", "reason": "unknown", "field": "string"}]},
    )

    with pytest.raises(AuraApiError, match="Internal Server Error"):
        api.delete_instance("id0")


def test_create_instance(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", project_id="some-tenant")

    mock_auth_token(requests_mock)

    requests_mock.get(
        "https://api.neo4j.io/v1/tenants/some-tenant",
        json={
            "data": {
                "id": "some_tenant",
                "instance_configurations": [{"type": "enterprise-ds", "region": "leipzig-1", "cloud_provider": "aws"}],
            }
        },
    )

    requests_mock.post(
        "https://api.neo4j.io/v1/instances",
        json={
            "data": {
                "id": "id0",
                "tenant_id": "some-tenant",
                "cloud_provider": "aws",
                "password": "foo",
                "username": "neo4j",
                "connection_url": "fake-url",
                "type": "",
                "region": "leipzig-1",
            }
        },
    )

    api.create_instance("name", SessionMemory.m_16GB.value, "gcp", "leipzig-1")

    requested_data = requests_mock.request_history[-1].json()
    assert requested_data["name"] == "name"
    assert requested_data["memory"] == "16GB"
    assert requested_data["cloud_provider"] == "gcp"
    assert requested_data["region"] == "leipzig-1"


def test_warn_about_expirying_endpoint(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", project_id="some-tenant")

    mock_auth_token(requests_mock)
    requests_mock.delete(
        "https://api.neo4j.io/v1/graph-analytics/sessions/id0",
        status_code=202,
        headers={"X-Tyk-Api-Expires": "Mon, 03 Mar 2025 00:00:00 UTC"},
    )

    with pytest.warns(DeprecationWarning):
        api.delete_session("id0")


def test_auth_token(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", project_id="some-tenant")

    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "very_short_token", "expires_in": 0, "token_type": "Bearer"},
    )

    assert api._request_session.auth._auth_token() == "very_short_token"  # type: ignore

    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "longer_token", "expires_in": 3600, "token_type": "Bearer"},
    )

    assert api._request_session.auth._auth_token() == "longer_token"  # type: ignore


def test_auth_token_reused(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", project_id="some-tenant")

    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "one_token", "expires_in": 3600, "token_type": "Bearer"},
    )

    assert api._request_session.auth._auth_token() == "one_token"  # type: ignore

    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "new_token", "expires_in": 3600, "token_type": "Bearer"},
    )

    # no new token requested
    assert api._request_session.auth._auth_token() == "one_token"  # type: ignore


def test_auth_token_use_short_token(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", project_id="some-tenant")

    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "one_token", "expires_in": 10, "token_type": "Bearer"},
    )

    assert api._request_session.auth._auth_token() == "one_token"  # type: ignore
    assert api._request_session.auth._auth_token() == "one_token"  # type: ignore


def test_derive_tenant(requests_mock: Mocker) -> None:
    mock_auth_token(requests_mock)

    requests_mock.get(
        "https://api.neo4j.io/v1/tenants",
        json={"data": [{"id": "6981ace7-efe8-4f5c-b7c5-267b5162ce91", "name": "Production"}]},
    )

    AuraApi(client_id="", client_secret="")


def test_raise_on_missing_tenant(requests_mock: Mocker) -> None:
    mock_auth_token(requests_mock)

    requests_mock.get(
        "https://api.neo4j.io/v1/tenants",
        json={
            "data": [
                {"id": "6981ace7-efe8-4f5c-b7c5-267b5162ce91", "name": "Production"},
                {"id": "YOUR_project_id", "name": "Staging"},
                {"id": "da045ab3-3b89-4f45-8b96-528f2e47cd13", "name": "Development"},
            ]
        },
    )

    with pytest.raises(RuntimeError, match="This account has access to multiple projects"):
        AuraApi(client_id="", client_secret="")


def test_list_instance(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", project_id="YOUR_project_id")

    mock_auth_token(requests_mock)
    requests_mock.get(
        "https://api.neo4j.io/v1/instances/id0",
        json={
            "data": {
                "id": "2f49c2b3",
                "name": "Production",
                "status": "running",
                "tenant_id": "YOUR_project_id",
                "cloud_provider": "gcp",
                "connection_url": "YOUR_CONNECTION_URL",
                "region": "europe-west1",
                "type": "enterprise-db",
                "memory": "8GB",
            }
        },
    )

    result = api.list_instance("id0")

    assert result and result.id == "2f49c2b3"
    assert result.cloud_provider == "gcp"
    assert result.region == "europe-west1"
    assert result.type == "enterprise-db"


def test_list_instance_missing_memory_field(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", project_id="some-tenant")

    mock_auth_token(requests_mock)
    requests_mock.get(
        "https://api.neo4j.io/v1/instances/id0",
        json={
            "data": {
                "cloud_provider": "gcp",
                "connection_url": None,
                "id": "a10fb995",
                "name": "gds-session-foo-bar",
                "region": "europe-west1",
                "status": "creating",
                "tenant_id": "046046d1-6996-53e4-8880-5b822766e1f9",
                "type": "enterprise-ds",
                "memory": "",
            }
        },
    )

    result = api.list_instance("id0")

    assert result and result.id == "a10fb995"
    assert result.memory == SESSION_MEMORY_VALUE_UNKNOWN


def test_list_missing_instance(requests_mock: Mocker) -> None:
    api = AuraApi("", "", project_id="some-tenant")

    mock_auth_token(requests_mock)

    requests_mock.get(
        "https://api.neo4j.io/v1/instances/id0",
        status_code=404,
        reason="Not Found",
        json={"errors": [{"message": "DB not found: id0", "reason": "db-not-found"}]},
    )

    assert api.list_instance("id0") is None


def test_list_instance_unknown_error(requests_mock: Mocker) -> None:
    api = AuraApi("", "", project_id="some-tenant")

    mock_auth_token(requests_mock)

    requests_mock.get(
        "https://api.neo4j.io/v1/instances/id0",
        status_code=500,
        reason="Not Found",
        text="my text",
    )

    with pytest.raises(
        AuraApiError,
        match="Request for https://api.neo4j.io/v1/instances/id0 failed with status code 500 - Not Found: `my text`'",
    ):
        api.list_instance("id0")


def test_list_instance_unknown_error_empty_body(requests_mock: Mocker) -> None:
    api = AuraApi("", "", project_id="some-tenant")

    mock_auth_token(requests_mock)

    requests_mock.get(
        "https://api.neo4j.io/v1/instances/id0",
        status_code=500,
        reason="Not Found",
    )

    with pytest.raises(
        AuraApiError,
        match="Request for https://api.neo4j.io/v1/instances/id0 failed with status code 500 - Not Found: ``",
    ):
        api.list_instance("id0")


def test_dont_wait_forever(requests_mock: Mocker, caplog: LogCaptureFixture) -> None:
    mock_auth_token(requests_mock)
    requests_mock.get(
        "https://api.neo4j.io/v1/instances/id0",
        json={
            "data": {
                "status": "creating",
                "cloud_provider": None,
                "connection_url": None,
                "id": None,
                "name": None,
                "region": None,
                "tenant_id": None,
                "type": None,
                "memory": "4Gi",
            }
        },
    )

    api = AuraApi("", "", project_id="some-tenant")

    with caplog.at_level(logging.DEBUG):
        assert (
            "Instance is not running after waiting for 0.7"
            in api.wait_for_instance_running("id0", max_wait_time=0.7).error
        )

    assert "Instance `id0` is not yet running. Current status: creating. Retrying in 0.2 seconds..." in caplog.text


def test_wait_for_instance_running(requests_mock: Mocker) -> None:
    mock_auth_token(requests_mock)
    requests_mock.get(
        "https://api.neo4j.io/v1/instances/id0",
        json={
            "data": {
                "status": "running",
                "cloud_provider": None,
                "connection_url": "foo.bar",
                "id": None,
                "name": None,
                "region": None,
                "tenant_id": None,
                "type": None,
                "memory": "4Gi",
            }
        },
    )

    api = AuraApi("", "", project_id="some-tenant")

    assert api.wait_for_instance_running("id0") == WaitResult.from_connection_url("foo.bar")


def test_wait_for_instance_deleting(requests_mock: Mocker) -> None:
    mock_auth_token(requests_mock)
    requests_mock.get(
        "https://api.neo4j.io/v1/instances/id0",
        json={
            "data": {
                "status": "deleting",
                "cloud_provider": None,
                "connection_url": None,
                "id": None,
                "name": None,
                "region": None,
                "tenant_id": None,
                "type": None,
                "memory": "4Gi",
            }
        },
    )
    requests_mock.get(
        "https://api.neo4j.io/v1/instances/id1",
        json={
            "data": {
                "status": "destroying",
                "cloud_provider": None,
                "connection_url": None,
                "id": None,
                "name": None,
                "region": None,
                "tenant_id": None,
                "type": None,
                "memory": "4Gi",
            }
        },
    )

    api = AuraApi("", "", project_id="some-tenant")

    assert api.wait_for_instance_running("id0") == WaitResult.from_error("Instance is being deleted")
    assert api.wait_for_instance_running("id1") == WaitResult.from_error("Instance is being deleted")


def test_estimate_size(requests_mock: Mocker) -> None:
    mock_auth_token(requests_mock)
    requests_mock.post(
        "https://api.neo4j.io/v1/sessions/sizing",
        json={"data": {"estimated_memory": "3070GB", "recommended_size": "512GB"}},
    )

    api = AuraApi("", "", project_id="some-tenant")
    assert api.estimate_size(100, 1, 1, 10, 1, [AlgorithmCategory.NODE_EMBEDDING]) == EstimationDetails(
        estimated_memory="3070GB", recommended_size="512GB"
    )


def test_extract_id() -> None:
    assert AuraApi.extract_id("neo4j+ssc://000fc8c8-envgdssync.databases.neo4j-dev.io") == "000fc8c8"
    assert AuraApi.extract_id("neo4j+ssc://02f1bff5.databases.neo4j.io") == "02f1bff5"


@pytest.mark.parametrize(
    "uri",
    ["", "some.neo4j.io", "02f1bff5"],
    ids=["empty string", "invalid", "id-only"],
)
def test_failing_extract_id(uri: str) -> None:
    with pytest.raises(RuntimeError, match="Could not parse the uri"):
        AuraApi.extract_id(uri)


def test_parse_create_details() -> None:
    InstanceCreateDetails.from_json({"id": "1", "username": "mats", "password": "1234", "connection_url": "url"})
    with pytest.raises(RuntimeError, match="Missing required field"):
        InstanceCreateDetails.from_json({"id": "1", "username": "mats", "password": "1234"})
    # too much is fine
    InstanceCreateDetails.from_json(
        {"id": "1", "username": "mats", "password": "1234", "connection_url": "url", "region": "fooo"}
    )


def test_parse_tenant_details() -> None:
    details = ProjectDetails.from_json(
        {
            "id": "42",
            "instance_configurations": [
                {"type": "enterprise-db", "region": "eu-west1", "cloud_provider": "aws"},
                {"type": "enterprise-ds", "region": "eu-west3", "cloud_provider": "gcp"},
                {"type": "enterprise-ds", "region": "us-central1", "cloud_provider": "aws"},
                {"type": "enterprise-ds", "region": "us-central3", "cloud_provider": "aws"},
            ],
        }
    )

    expected_details = ProjectDetails(
        "42",
        cloud_locations={
            CloudLocation("gcp", "eu-west3"),
            CloudLocation("aws", "eu-west1"),
            CloudLocation("aws", "us-central1"),
            CloudLocation("aws", "us-central3"),
        },
    )
    assert details == expected_details


def test_parse_session_info() -> None:
    session_details = {
        "id": "test_id",
        "name": "test_session",
        "memory": "4Gi",
        "instance_id": "test_instance",
        "status": "running",
        "expiry_date": "2022-01-01T00:00:00Z",
        "created_at": "2021-01-01T00:00:00Z",
        "host": "a.b",
        "ttl": "1d8h1m2s",
        "project_id": "tenant-1",
        "user_id": "user-1",
    }
    session_info = SessionDetails.from_json(session_details)

    assert session_info == SessionDetails(
        id="test_id",
        name="test_session",
        memory=SessionMemory.m_4GB.value,
        instance_id="test_instance",
        status="running",
        host="a.b",
        expiry_date=datetime(2022, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        created_at=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        ttl=timedelta(days=1, hours=8, minutes=1, seconds=2),
        project_id="tenant-1",
        user_id="user-1",
    )


def test_parse_session_info_without_optionals() -> None:
    session_details = {
        "id": "test_id",
        "name": "test_session",
        "memory": "16Gi",
        "instance_id": "test_instance",
        "status": "running",
        "host": "a.b",
        "created_at": "2021-01-01T00:00:00Z",
        "project_id": "tenant-1",
        "user_id": "user-1",
    }
    session_info = SessionDetails.from_json(session_details)

    assert session_info == SessionDetails(
        id="test_id",
        name="test_session",
        memory=SessionMemory.m_16GB.value,
        instance_id="test_instance",
        host="a.b",
        status="running",
        expiry_date=None,
        ttl=None,
        created_at=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        project_id="tenant-1",
        user_id="user-1",
    )


def test_estimate_size_parsing() -> None:
    assert EstimationDetails._parse_size("8GB") == 8589934592
    assert EstimationDetails._parse_size("8G") == 8589934592
    assert EstimationDetails._parse_size("512MB") == 536870912
    assert EstimationDetails._parse_size("256KB") == 262144
    assert EstimationDetails._parse_size("1024B") == 1024
    assert EstimationDetails._parse_size("12345") == 12345


def test_estimate_exceeds_maximum() -> None:
    estimation = EstimationDetails(estimated_memory="16Gi", recommended_size="8Gi")
    assert estimation.exceeds_recommended() is True

    estimation = EstimationDetails(estimated_memory="8Gi", recommended_size="16Gi")
    assert estimation.exceeds_recommended() is False
