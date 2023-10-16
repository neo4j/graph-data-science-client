import dataclasses
import logging
from dataclasses import asdict
from typing import Any, List, Optional

import pytest
from pytest_mock import MockerFixture
from requests_mock import Mocker

from graphdatascience.aura_api import (
    AuraApi,
    InstanceCreateDetails,
    InstanceDetails,
    InstanceSpecificDetails,
)
from graphdatascience.aura_sessions import AuraSessions, SessionInfo
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.aura_db_arrow_query_runner import (
    AuraDbConnectionInfo,
)


class CollectingAuraApi(AuraApi):
    def __init__(self, existing_instances: Optional[List[InstanceSpecificDetails]] = None) -> None:
        if existing_instances is None:
            existing_instances = []
        self._instances = {details.id: details for details in existing_instances}
        self.id_counter = 0
        self.time = 0
        self._logger = logging.getLogger()

    def create_instance(self, name: str) -> InstanceCreateDetails:
        create_details = InstanceCreateDetails(
            id=f"id-{self.id_counter}",
            name=name,
            tenant_id="tenant_id",
            cloud_provider="cloud_provider",
            username="gds-user",
            password="gds-pw",
            connection_url="gds-url",
        )

        specific_details = InstanceSpecificDetails(
            id=create_details.id,
            name=create_details.name,
            tenant_id=create_details.tenant_id,
            cloud_provider=create_details.cloud_provider,
            status="CREATING",
            connection_url=create_details.connection_url,
            memory="",
        )

        self.id_counter += 1
        self._instances[specific_details.id] = specific_details

        return create_details

    def delete_instance(self, instance_id: str) -> InstanceSpecificDetails:
        return self._instances.pop(instance_id)

    def list_instances(self) -> List[InstanceDetails]:
        return [v for _, v in self._instances.items()]

    def list_instance(self, instance_id: str) -> Optional[InstanceSpecificDetails]:
        matched_instances = self._instances.get(instance_id, None)

        if matched_instances:
            old_instance = matched_instances
            self._instances[instance_id] = dataclasses.replace(old_instance, status="RUNNING")
            return old_instance
        else:
            return None


@pytest.fixture
def aura_api() -> AuraApi:
    return CollectingAuraApi()


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


def test_create_session_(mocker: MockerFixture, gds: GraphDataScience, aura_api: AuraApi) -> None:
    db_credentials = AuraDbConnectionInfo("db-uri", ("dbuser", "db_pw"))
    sessions = AuraSessions(db_credentials, aura_api_client_auth=("", ""))
    sessions._aura_api = aura_api

    gds_user = "gds-user"
    gds_pw = "gds-pw"
    gds_url = "gds-url"

    def assert_credentials(*args: List[Any], **kwargs: dict[str, Any]) -> GraphDataScience:
        assert kwargs == {"gds_url": gds_url, "gds_user": gds_user, "gds_pw": gds_pw}
        return gds

    mocker.patch("graphdatascience.aura_sessions.AuraSessions._construct_client", assert_credentials)

    gds = sessions.create_gds("my-session")

    expected_instance_name = "gds-session-my-session"
    sessions.list_sessions() == [SessionInfo(expected_instance_name)]
