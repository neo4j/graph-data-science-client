import pytest
from requests import HTTPError
from requests_mock import Mocker

from graphdatascience.aura_api import AuraApi, InstanceSpecificDetails


def test_delete_instance(requests_mock: Mocker):
    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "my_token", "expires_in": 3600, "token_type": "Bearer"},
    )

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

    api = AuraApi("", "")

    result = api.delete_instance("id0")

    assert result == InstanceSpecificDetails(
        id="id0", name="", tenant_id="", cloud_provider="", status="deleting", connection_url="", memory=""
    )


def test_delete_already_deleting_instance(requests_mock: Mocker):
    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "my_token", "expires_in": 3600, "token_type": "Bearer"},
    )
    requests_mock.delete(
        "https://api.neo4j.io/v1/instances/id0",
        status_code=404,
        reason="Not Found",
        json={"errors": [{"message": "DB not found: id0", "reason": "db-not-found"}]},
    )

    api = AuraApi("", "")

    result = api.delete_instance("id0")
    assert result is None


def test_delete_that_fails(requests_mock: Mocker):
    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "my_token", "expires_in": 3600, "token_type": "Bearer"},
    )

    requests_mock.delete(
        "https://api.neo4j.io/v1/instances/id0",
        status_code=500,
        reason="Internal Server Error",
        json={"errors": [{"message": "some failure happened", "reason": "unknown", "field": "string"}]},
    )

    api = AuraApi("", "")

    with pytest.raises(HTTPError, match="Internal Server Error"):
        api.delete_instance("id0")

def test_auth_token(requests_mock):
    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "very_short_token", "expires_in": 1, "token_type": "Bearer"},
    )

    api = AuraApi("", "")

    assert api._auth_token() == "very_short_token"

    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "longer_token", "expires_in": 3600, "token_type": "Bearer"},
    )

    assert api._auth_token() == "longer_token"
