import pytest
from requests import HTTPError
from requests_mock import Mocker

from graphdatascience.aura_api import AuraApi, InstanceSpecificDetails


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
        id="id0", name="", tenant_id="", cloud_provider="", status="deleting", connection_url="", memory=""
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
    # happy case
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
