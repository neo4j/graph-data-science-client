import logging
from datetime import datetime, timedelta, timezone

import pytest
from _pytest.logging import LogCaptureFixture
from requests_mock import Mocker
from requests_mock.request import _RequestObjectProxy

from graphdatascience.session import SessionMemory
from graphdatascience.session.algorithm_category import AlgorithmCategory
from graphdatascience.session.aura_api import AuraApi, AuraApiError
from graphdatascience.session.aura_api_responses import (
    EstimationDetails,
    InstanceCreateDetails,
    InstanceSpecificDetails,
    SessionDetails,
    TenantDetails,
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
    api = AuraApi(client_id="", client_secret="", tenant_id="some-tenant")

    mock_auth_token(requests_mock)

    def assert_body(request: _RequestObjectProxy) -> bool:
        assert request.json() == {
            "name": "name-0",
            "password": "pwd-2",
            "memory": "4GB",
            "instance_id": "dbid-1",
            "ttl": "42.0s",
            "tenant_id": "some-tenant",
        }
        return True

    requests_mock.post(
        "https://api.neo4j.io/v1beta5/data-science/sessions",
        json={
            "data": {
                "id": "id0",
                "name": "name-0",
                "status": "Creating",
                "instance_id": "dbid-1",
                "created_at": "1970-01-01T00:00:00Z",
                "host": "1.2.3.4",
                "memory": "4Gi",
                "tenant_id": "some-tenant",
                "user_id": "user-0",
                "ttl": "42s",
            }
        },
        additional_matcher=assert_body,
    )

    result = api.create_session(
        name="name-0", dbid="dbid-1", pwd="pwd-2", memory=SessionMemory.m_4GB.value, ttl=timedelta(seconds=42)
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
        tenant_id="some-tenant",
        user_id="user-0",
    )


def test_create_dedicated_session(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", tenant_id="some-tenant")

    mock_auth_token(requests_mock)

    def assert_body(request: _RequestObjectProxy) -> bool:
        assert request.json() == {
            "name": "name-0",
            "tenant_id": "some-tenant",
            "password": "pwd-2",
            "memory": "4GB",
            "cloud_provider": "aws",
            "region": "leipzig-1",
            "ttl": "42.0s",
        }
        return True

    requests_mock.post(
        "https://api.neo4j.io/v1beta5/data-science/sessions",
        json={
            "data": {
                "id": "id0",
                "name": "name-0",
                "status": "Creating",
                "instance_id": "",
                "created_at": "1970-01-01T00:00:00Z",
                "host": "1.2.3.4",
                "memory": "4Gi",
                "tenant_id": "tenant-0",
                "user_id": "user-0",
                "ttl": "42.0s",
            }
        },
        additional_matcher=assert_body,
    )

    result = api.create_session(
        "name-0",
        "pwd-2",
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
        tenant_id="tenant-0",
        user_id="user-0",
    )


def test_create_standalone_session_error_forwards(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", tenant_id="some-tenant")

    mock_auth_token(requests_mock)

    requests_mock.post(
        "https://api.neo4j.io/v1beta5/data-science/sessions",
        status_code=400,
        json={
            "errors": {
                "message": "some validation error",
            }
        },
    )

    with pytest.raises(AuraApiError, match="some validation error"):
        api.create_session(
            "name-0",
            "pwd-2",
            SessionMemory.m_4GB.value,
            ttl=timedelta(seconds=42),
            cloud_location=CloudLocation("invalidProvider", "leipzig-1"),
        )


def test_get_session(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", tenant_id="some-tenant")

    mock_auth_token(requests_mock)

    requests_mock.get(
        "https://api.neo4j.io/v1beta5/data-science/sessions/id0",
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
                "tenant_id": "tenant-0",
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
        tenant_id="tenant-0",
        user_id="user-0",
    )


def test_list_sessions(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", tenant_id="some-tenant")
    mock_auth_token(requests_mock)

    requests_mock.get(
        "https://api.neo4j.io/v1beta5/data-science/sessions?tenantId=some-tenant",
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
                    "tenant_id": "tenant-1",
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
                    "tenant_id": "tenant-2",
                    "user_id": "user-2",
                },
            ]
        },
    )

    result = api.list_sessions()

    expected1 = SessionDetails(
        id="id0",
        name="name-0",
        status="Ready",
        instance_id="dbid-1",
        created_at=TimeParser.fromisoformat("1970-01-01T00:00:00Z"),
        host="1.2.3.4",
        memory=SessionMemory.m_4GB.value,
        expiry_date=TimeParser.fromisoformat("1977-01-01T00:00:00Z"),
        ttl=None,
        tenant_id="tenant-1",
        user_id="user-1",
        cloud_location=CloudLocation("gcp", "leipzig"),
    )

    expected2 = SessionDetails(
        id="id1",
        name="name-2",
        status="Creating",
        instance_id="dbid-3",
        created_at=TimeParser.fromisoformat("2012-01-01T00:00:00Z"),
        memory=SessionMemory.m_8GB.value,
        host="foo.bar",
        expiry_date=None,
        ttl=None,
        tenant_id="tenant-2",
        user_id="user-2",
        cloud_location=None,
    )

    assert result == [expected1, expected2]


def test_list_sessions_with_db_id(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", tenant_id="some-tenant")
    mock_auth_token(requests_mock)

    requests_mock.get(
        "https://api.neo4j.io/v1beta5/data-science/sessions?tenantId=some-tenant&instanceId=dbid",
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
                    "tenant_id": "tenant-1",
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
                    "tenant_id": "tenant-2",
                    "user_id": "user-2",
                },
            ]
        },
    )

    result = api.list_sessions("dbid")

    expected1 = SessionDetails(
        id="id0",
        name="name-0",
        status="Ready",
        instance_id="dbid-1",
        created_at=TimeParser.fromisoformat("1970-01-01T00:00:00Z"),
        host="1.2.3.4",
        memory=SessionMemory.m_4GB.value,
        expiry_date=TimeParser.fromisoformat("1977-01-01T00:00:00Z"),
        ttl=None,
        tenant_id="tenant-1",
        user_id="user-1",
    )

    expected2 = SessionDetails(
        id="id1",
        name="name-2",
        status="Creating",
        instance_id="dbid-3",
        created_at=TimeParser.fromisoformat("2012-01-01T00:00:00Z"),
        memory=SessionMemory.m_8GB.value,
        host="foo.bar",
        expiry_date=None,
        ttl=None,
        tenant_id="tenant-2",
        user_id="user-2",
    )

    assert result == [expected1, expected2]


def test_delete_session(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", tenant_id="some-tenant")

    mock_auth_token(requests_mock)
    requests_mock.delete(
        "https://api.neo4j.io/v1beta5/data-science/sessions/id0",
        status_code=202,
    )

    assert api.delete_session("id0") is True


def test_delete_missing_session(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", tenant_id="some-tenant")

    mock_auth_token(requests_mock)
    requests_mock.delete(
        "https://api.neo4j.io/v1beta5/data-science/sessions/id0",
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
        match="This account has access to multiple tenants: `{'tenant1': 'Production', 'tenant2': 'Development'}`",
    ):
        AuraApi(client_id="", client_secret="")


def test_dont_wait_forever_for_session(requests_mock: Mocker, caplog: LogCaptureFixture) -> None:
    mock_auth_token(requests_mock)
    requests_mock.get(
        "https://api.neo4j.io/v1beta5/data-science/sessions/id0",
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
                "tenant_id": "tenant-1",
                "user_id": "user-1",
            }
        },
    )

    api = AuraApi("", "", tenant_id="some-tenant")

    with caplog.at_level(logging.DEBUG):
        assert (
            "Session `id0` is not running after 0.7 seconds"
            in api.wait_for_session_running("id0", max_wait_time=0.7).error
        )

    assert "Session `id0` is not yet running. Current status: Creating Host: foo.bar. Retrying in 0.2" in caplog.text


def test_wait_for_session_running(requests_mock: Mocker) -> None:
    mock_auth_token(requests_mock)
    requests_mock.get(
        "https://api.neo4j.io/v1beta5/data-science/sessions/id0",
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
                "tenant_id": "tenant-1",
                "user_id": "user-1",
            }
        },
    )

    api = AuraApi("", "", tenant_id="some-tenant")

    assert api.wait_for_session_running("id0") == WaitResult.from_connection_url("neo4j+s://foo.bar")


def test_delete_instance(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", tenant_id="some-tenant")

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
        tenant_id="",
        cloud_provider="",
        status="deleting",
        connection_url="",
        memory=SessionMemory.m_4GB.value,
        region="",
        type="",
    )


def test_delete_already_deleting_instance(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", tenant_id="some-tenant")

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
    api = AuraApi(client_id="", client_secret="", tenant_id="some-tenant")

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
    api = AuraApi(client_id="", client_secret="", tenant_id="some-tenant")

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
                "username": "neo4j",
                "password": "fake-pw",
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
    api = AuraApi(client_id="", client_secret="", tenant_id="some-tenant")

    mock_auth_token(requests_mock)
    requests_mock.delete(
        "https://api.neo4j.io/v1beta5/data-science/sessions/id0",
        status_code=202,
        headers={"X-Tyk-Api-Expires": "Mon, 03 Mar 2025 00:00:00 UTC"},
    )

    with pytest.warns(DeprecationWarning):
        api.delete_session("id0")


def test_auth_token(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", tenant_id="some-tenant")

    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "very_short_token", "expires_in": 0, "token_type": "Bearer"},
    )

    assert api._request_session.auth._auth_token() == "very_short_token"

    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "longer_token", "expires_in": 3600, "token_type": "Bearer"},
    )

    assert api._request_session.auth._auth_token() == "longer_token"


def test_auth_token_reused(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", tenant_id="some-tenant")

    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "one_token", "expires_in": 3600, "token_type": "Bearer"},
    )

    assert api._request_session.auth._auth_token() == "one_token"

    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "new_token", "expires_in": 3600, "token_type": "Bearer"},
    )

    # no new token requested
    assert api._request_session.auth._auth_token() == "one_token"


def test_auth_token_use_short_token(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", tenant_id="some-tenant")

    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "one_token", "expires_in": 10, "token_type": "Bearer"},
    )

    assert api._request_session.auth._auth_token() == "one_token"
    assert api._request_session.auth._auth_token() == "one_token"


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
                {"id": "YOUR_TENANT_ID", "name": "Staging"},
                {"id": "da045ab3-3b89-4f45-8b96-528f2e47cd13", "name": "Development"},
            ]
        },
    )

    with pytest.raises(RuntimeError, match="This account has access to multiple tenants"):
        AuraApi(client_id="", client_secret="")


def test_list_instance(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", tenant_id="YOUR_TENANT_ID")

    mock_auth_token(requests_mock)
    requests_mock.get(
        "https://api.neo4j.io/v1/instances/id0",
        json={
            "data": {
                "id": "2f49c2b3",
                "name": "Production",
                "status": "running",
                "tenant_id": "YOUR_TENANT_ID",
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
    api = AuraApi(client_id="", client_secret="", tenant_id="some-tenant")

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
    api = AuraApi("", "", tenant_id="some-tenant")

    mock_auth_token(requests_mock)

    requests_mock.get(
        "https://api.neo4j.io/v1/instances/id0",
        status_code=404,
        reason="Not Found",
        json={"errors": [{"message": "DB not found: id0", "reason": "db-not-found"}]},
    )

    assert api.list_instance("id0") is None


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

    api = AuraApi("", "", tenant_id="some-tenant")

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

    api = AuraApi("", "", tenant_id="some-tenant")

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

    api = AuraApi("", "", tenant_id="some-tenant")

    assert api.wait_for_instance_running("id0") == WaitResult.from_error("Instance is being deleted")
    assert api.wait_for_instance_running("id1") == WaitResult.from_error("Instance is being deleted")


def test_estimate_size(requests_mock: Mocker) -> None:
    mock_auth_token(requests_mock)
    requests_mock.post(
        "https://api.neo4j.io/v1/instances/sizing",
        json={"data": {"did_exceed_maximum": True, "min_required_memory": "307GB", "recommended_size": "96GB"}},
    )

    api = AuraApi("", "", tenant_id="some-tenant")
    assert api.estimate_size(100, 10, [AlgorithmCategory.NODE_EMBEDDING]) == EstimationDetails("307GB", "96GB", True)


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
    details = TenantDetails.from_json(
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

    expected_details = TenantDetails(
        "42", ds_type="enterprise-ds", regions_per_provider={"gcp": {"eu-west3"}, "aws": {"us-central1", "us-central3"}}
    )
    assert details == expected_details


def test_parse_non_ds_details() -> None:
    with pytest.raises(
        RuntimeError,
        match="Tenant with id `42` cannot create DS instances. Available instances are `{'enterprise-db'}`.",
    ):
        TenantDetails.from_json(
            {
                "id": "42",
                "instance_configurations": [
                    {"type": "enterprise-db", "region": "europe-west1", "cloud_provider": "aws"}
                ],
            }
        )


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
        "tenant_id": "tenant-1",
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
        tenant_id="tenant-1",
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
        "tenant_id": "tenant-1",
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
        tenant_id="tenant-1",
        user_id="user-1",
    )
