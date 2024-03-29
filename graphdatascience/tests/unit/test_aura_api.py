import logging

import pytest
from _pytest.logging import LogCaptureFixture
from requests import HTTPError
from requests_mock import Mocker

from graphdatascience.session.aura_api import (
    AuraApi,
    InstanceCreateDetails,
    InstanceSpecificDetails,
    TenantDetails,
    WaitResult,
)


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
                "memory": "",
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
        memory="",
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

    with pytest.raises(HTTPError, match="Internal Server Error"):
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

    api.create_instance("name", "16GB", "gcp", "leipzig-1")

    requested_data = requests_mock.request_history[-1].json()
    assert requested_data["name"] == "name"
    assert requested_data["memory"] == "16GB"
    assert requested_data["cloud_provider"] == "gcp"
    assert requested_data["region"] == "leipzig-1"


def test_auth_token(requests_mock: Mocker) -> None:
    api = AuraApi(client_id="", client_secret="", tenant_id="some-tenant")

    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "very_short_token", "expires_in": 1, "token_type": "Bearer"},
    )

    assert api._auth_token() == "very_short_token"

    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "longer_token", "expires_in": 3600, "token_type": "Bearer"},
    )

    assert api._auth_token() == "longer_token"


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
            }
        },
    )

    result = api.list_instance("id0")

    assert result and result.id == "a10fb995"
    assert result.memory == ""


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


def mock_auth_token(requests_mock: Mocker) -> None:
    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "very_short_token", "expires_in": 500, "token_type": "Bearer"},
    )


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
            }
        },
    )

    api = AuraApi("", "", tenant_id="some-tenant")

    with caplog.at_level(logging.DEBUG):
        assert (
            "Instance is not running after waiting for 0.8"
            in api.wait_for_instance_running("id0", max_sleep_time=0.7).error
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
            }
        },
    )

    api = AuraApi("", "", tenant_id="some-tenant")

    assert api.wait_for_instance_running("id0") == WaitResult.from_error("Instance is being deleted")
    assert api.wait_for_instance_running("id1") == WaitResult.from_error("Instance is being deleted")


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
