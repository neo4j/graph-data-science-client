from dataclasses import asdict
from typing import Any, List

from pytest_mock import MockerFixture
from requests_mock import Mocker

from graphdatascience.aura_api import InstanceCreateDetails, InstanceDetails
from graphdatascience.aura_sessions import AuraSessions, SessionInfo
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.aura_db_arrow_query_runner import (
    AuraDbConnectionInfo,
)


def test_list_session(requests_mock: Mocker) -> None:
    sessions = AuraSessions(db_credentials=AuraDbConnectionInfo("", ("", "")), aura_api_client_auth=("", ""))

    db_instance = InstanceDetails(id="id", name="Instance01", tenant_id="tenant_id", cloud_provider="cloud_provider")
    gds_instance = InstanceDetails(
        id="id", name="gds-session-my-session-name", tenant_id="tenant_id", cloud_provider="cloud_provider"
    )

    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "my_token", "expires_in": 3600, "token_type": "Bearer"},
    )
    requests_mock.get(
        "https://api.neo4j.io/v1/instances", json={"data": [asdict(i) for i in [db_instance, gds_instance]]}
    )

    assert sessions.list_sessions() == [SessionInfo("gds-session-my-session-name")]


def test_create_session(mocker: MockerFixture, requests_mock: Mocker, gds: GraphDataScience) -> None:
    db_credentials = AuraDbConnectionInfo("db-uri", ("dbuser", "db_pw"))
    sessions = AuraSessions(db_credentials, aura_api_client_auth=("", ""))
    expected_instance_name = "gds-session-my-session"

    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "my_token", "expires_in": 3600, "token_type": "Bearer"},
    )
    requests_mock.get("https://api.neo4j.io/v1/tenants", json={"data": [{"id": "my_tenant"}]})

    gds_user = "gds-user"
    gds_pw = "gds-pw"
    gds_url = "gds-url"
    create_response = InstanceCreateDetails(
        id="id",
        name=expected_instance_name,
        tenant_id="tenant_id",
        cloud_provider="cloud_provider",
        username=gds_user,
        password=gds_pw,
        connection_url=gds_url,
    )
    requests_mock.post(
        "https://api.neo4j.io/v1/instances",
        headers={"name": expected_instance_name},
        json={"data": asdict(create_response)},
    )

    def assert_credentials(*args: List[Any], **kwargs: dict[str, Any]) -> GraphDataScience:
        assert kwargs == {"gds_url": gds_url, "gds_user": gds_user, "gds_pw": gds_pw}
        return gds

    mocker.patch("graphdatascience.aura_sessions.AuraSessions._construct_client", assert_credentials)

    gds = sessions.create_gds("my-session")
