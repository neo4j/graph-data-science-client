import dataclasses
import re
from typing import Any, List, Optional

import pytest
from pytest_mock import MockerFixture

from graphdatascience.session.algorithm_category import AlgorithmCategory
from graphdatascience.session.aura_api import AuraApi
from graphdatascience.session.aura_api_responses import (
    EstimationDetails,
    InstanceCreateDetails,
    InstanceDetails,
    InstanceSpecificDetails,
    TenantDetails,
    WaitResult,
)
from graphdatascience.session.aurads_sessions import AuraDsSessions, SessionNameHelper
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.session.session_info import SessionInfo
from graphdatascience.session.session_sizes import SessionMemory


class FakeAuraApi(AuraApi):
    def __init__(
        self,
        existing_instances: Optional[List[InstanceSpecificDetails]] = None,
        status_after_creating: str = "running",
        size_estimation: Optional[EstimationDetails] = None,
    ) -> None:
        super().__init__("", "", "tenant_id")
        if existing_instances is None:
            existing_instances = []
        self._instances = {details.id: details for details in existing_instances}
        self.id_counter = 0
        self.time = 0
        self._status_after_creating = status_after_creating
        self._size_estimation = size_estimation or EstimationDetails("1GB", "8GB", False)

    def create_instance(self, name: str, memory: str, cloud_provider: str, region: str) -> InstanceCreateDetails:
        create_details = InstanceCreateDetails(
            id=f"ffff{self.id_counter}",
            username="neo4j",
            password="fake-pw",
            connection_url="fake-url",
        )

        specific_details = InstanceSpecificDetails(
            id=create_details.id,
            status="creating",
            connection_url="fake-url",
            memory=memory,
            type="",
            region=region,
            name=name,
            tenant_id=self._tenant_id,
            cloud_provider=cloud_provider,
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
            self._instances[instance_id] = dataclasses.replace(old_instance, status=self._status_after_creating)
            return old_instance
        else:
            return None

    def wait_for_instance_running(
        self,
        instance_id: str,
        sleep_time: float = 0.2,
        max_sleep_time: float = 5,
        max_wait_time: float = 5,
    ) -> WaitResult:
        return super().wait_for_instance_running(instance_id, sleep_time=0.0001, max_wait_time=0.001)

    def tenant_details(self) -> TenantDetails:
        return TenantDetails(id=self._tenant_id, ds_type="fake-ds", regions_per_provider={"aws": {"leipzig-1"}})

    def estimate_size(
        self, node_count: int, relationship_count: int, algorithm_categories: List[AlgorithmCategory]
    ) -> EstimationDetails:
        return self._size_estimation


@pytest.fixture
def aura_api() -> AuraApi:
    return FakeAuraApi()


def test_list_session(aura_api: AuraApi) -> None:
    aura_api.create_instance(name="gds-session-my-session-name", cloud_provider="gcp", memory="16GB", region="")

    sessions = AuraDsSessions(aura_api)

    assert sessions.list() == [SessionInfo("my-session-name", "16GB")]


def test_create_session(mocker: MockerFixture, aura_api: AuraApi) -> None:
    _setup_db_instance(aura_api)

    sessions = AuraDsSessions(aura_api)

    def assert_db_credentials(*args: List[Any], **kwargs: str) -> None:
        assert kwargs == {"gds_url": "fake-url", "gds_user": "neo4j", "initial_pw": "fake-pw", "new_pw": "db_pw"}

    mocker.patch(
        "graphdatascience.session.aurads_sessions.AuraDsSessions._construct_client", lambda *args, **kwargs: kwargs
    )
    mocker.patch("graphdatascience.session.aurads_sessions.AuraDsSessions._change_initial_pw", assert_db_credentials)

    gds_credentials = sessions.get_or_create(
        "my-session",
        SessionMemory.m_384GB,
        DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "dbuser", "db_pw"),
    )

    assert gds_credentials == {  # type: ignore
        "db_connection": DbmsConnectionInfo(
            uri="neo4j+ssc://ffff0.databases.neo4j.io", username="dbuser", password="db_pw"
        ),
        "gds_url": "fake-url",
        "session_name": "my-session",
    }
    assert sessions.list() == [SessionInfo("my-session", "384GB")]

    instance_details: InstanceSpecificDetails = aura_api.list_instance("ffff1")  # type: ignore
    assert instance_details.cloud_provider == "aws"
    assert instance_details.region == "leipzig-1"
    assert instance_details.memory == "384GB"


def test_create_default_session(mocker: MockerFixture, aura_api: AuraApi) -> None:
    _setup_db_instance(aura_api)

    sessions = AuraDsSessions(aura_api)

    mocker.patch(
        "graphdatascience.session.aurads_sessions.AuraDsSessions._construct_client", lambda *args, **kwargs: kwargs
    )
    mocker.patch(
        "graphdatascience.session.aurads_sessions.AuraDsSessions._change_initial_pw", lambda *args, **kwargs: kwargs
    )

    sessions.get_or_create(
        "my-session",
        SessionMemory.m_8GB,
        DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "dbuser", "db_pw"),
    )
    instance_details: InstanceSpecificDetails = aura_api.list_instance("ffff1")  # type: ignore
    assert instance_details.cloud_provider == "aws"
    assert instance_details.region == "leipzig-1"
    assert instance_details.memory == "8GB"


def test_create_session_override_region(mocker: MockerFixture, aura_api: AuraApi) -> None:
    aura_api.create_instance("test", "8GB", "aws", "dresden-2")

    sessions = AuraDsSessions(aura_api)

    mocker.patch(
        "graphdatascience.session.aurads_sessions.AuraDsSessions._construct_client", lambda *args, **kwargs: kwargs
    )
    mocker.patch(
        "graphdatascience.session.aurads_sessions.AuraDsSessions._change_initial_pw", lambda *args, **kwargs: kwargs
    )

    sessions.get_or_create(
        "my-session",
        SessionMemory.m_8GB,
        DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "dbuser", "db_pw"),
    )
    instance_details: InstanceSpecificDetails = aura_api.list_instance("ffff1")  # type: ignore
    assert instance_details.cloud_provider == "aws"
    assert instance_details.region == "leipzig-1"
    assert instance_details.memory == "8GB"


def test_get_or_create(mocker: MockerFixture, aura_api: AuraApi) -> None:
    _setup_db_instance(aura_api)

    sessions = AuraDsSessions(aura_api)

    mocker.patch(
        "graphdatascience.session.aurads_sessions.AuraDsSessions._construct_client", lambda *args, **kwargs: kwargs
    )
    mocker.patch(
        "graphdatascience.session.aurads_sessions.AuraDsSessions._change_initial_pw", lambda *args, **kwargs: None
    )

    gds_args1 = sessions.get_or_create(
        "my-session",
        SessionMemory.m_8GB,
        DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "dbuser", "db_pw"),
    )
    gds_args2 = sessions.get_or_create(
        "my-session",
        SessionMemory.m_8GB,
        DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "dbuser", "db_pw"),
    )

    assert gds_args1 == {  # type: ignore
        "db_connection": DbmsConnectionInfo(
            uri="neo4j+ssc://ffff0.databases.neo4j.io", username="dbuser", password="db_pw"
        ),
        "gds_url": "fake-url",
        "session_name": "my-session",
    }
    assert gds_args1 == gds_args2

    assert sessions.list() == [SessionInfo("my-session", "8GB")]


def test_get_or_create_different_size(mocker: MockerFixture, aura_api: AuraApi) -> None:
    _setup_db_instance(aura_api)

    sessions = AuraDsSessions(aura_api)

    mocker.patch(
        "graphdatascience.session.aurads_sessions.AuraDsSessions._construct_client", lambda *args, **kwargs: kwargs
    )
    mocker.patch(
        "graphdatascience.session.aurads_sessions.AuraDsSessions._change_initial_pw", lambda *args, **kwargs: None
    )

    sessions.get_or_create(
        "my-session",
        SessionMemory.m_8GB,
        DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "dbuser", "db_pw"),
    )

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Session `my-session` already exists with memory `8GB`." " Requested memory `32GB` does not match."
        ),
    ):
        sessions.get_or_create(
            "my-session",
            SessionMemory.m_32GB,
            DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "dbuser", "db_pw"),
        )


def test_get_or_create_duplicate_session() -> None:
    aura_api = FakeAuraApi(
        existing_instances=[
            InstanceSpecificDetails(
                id="id42",
                name=SessionNameHelper.instance_name("one"),
                tenant_id="",
                cloud_provider="",
                status="RUNNING",
                connection_url="",
                memory="",
                type="",
                region="",
            ),
            InstanceSpecificDetails(
                id="id43",
                name=SessionNameHelper.instance_name("one"),
                tenant_id="",
                cloud_provider="",
                status="RUNNING",
                connection_url="",
                memory="",
                type="",
                region="",
            ),
        ]
    )

    sessions = AuraDsSessions(aura_api)

    with pytest.raises(RuntimeError, match=re.escape("Expected to find exactly one GDS session with name `one`")):
        sessions.get_or_create("one", SessionMemory.m_8GB, DbmsConnectionInfo("", "", ""))


def test_delete_session() -> None:
    existing_instances = [
        InstanceSpecificDetails(
            id="id42",
            name=SessionNameHelper.instance_name("one"),
            tenant_id="tenant_id",
            cloud_provider="cloud_provider",
            status="RUNNING",
            connection_url="",
            memory="",
            type="",
            region="",
        ),
        InstanceSpecificDetails(
            id="id1",
            name=SessionNameHelper.instance_name("other"),
            tenant_id="tenant_id",
            cloud_provider="cloud_provider",
            status="RUNNING",
            connection_url="",
            memory="",
            type="",
            region="",
        ),
    ]

    sessions = AuraDsSessions(FakeAuraApi(existing_instances=existing_instances))

    assert sessions.delete("one")
    assert sessions.list() == [SessionInfo("other", "")]


def test_delete_nonexisting_session() -> None:
    existing_instances = [
        InstanceSpecificDetails(
            id="id42",
            name=SessionNameHelper.instance_name("one"),
            tenant_id="tenant_id",
            cloud_provider="cloud_provider",
            status="RUNNING",
            connection_url="",
            memory="",
            type="",
            region="",
        ),
    ]

    sessions = AuraDsSessions(FakeAuraApi(existing_instances=existing_instances))

    assert sessions.delete("other") is False
    assert sessions.list() == [SessionInfo("one", "")]


def test_delete_nonunique_session() -> None:
    existing_instances = [
        InstanceSpecificDetails(
            id="id42",
            name=SessionNameHelper.instance_name("one"),
            tenant_id="",
            cloud_provider="",
            status="RUNNING",
            connection_url="",
            memory="",
            type="",
            region="",
        ),
        InstanceSpecificDetails(
            id="id43",
            name=SessionNameHelper.instance_name("one"),
            tenant_id="",
            cloud_provider="",
            status="RUNNING",
            connection_url="",
            memory="",
            type="",
            region="",
        ),
    ]

    sessions = AuraDsSessions(FakeAuraApi(existing_instances=existing_instances))
    with pytest.raises(
        RuntimeError,
        match=re.escape(
            "Expected to find exactly one GDS session with name `one`,"
            " but found `[('id42', 'one'), ('id43', 'one')]`."
        ),
    ):
        sessions.delete("one")

    assert sessions.list() == [SessionInfo("one", ""), SessionInfo("one", "")]


def test_create_immediate_delete() -> None:
    aura_api = FakeAuraApi(status_after_creating="deleting")
    _setup_db_instance(aura_api)
    sessions = AuraDsSessions(aura_api)

    with pytest.raises(RuntimeError, match="Failed to create session `one`: Instance is being deleted"):
        sessions.get_or_create(
            "one", SessionMemory.m_8GB, DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "", "")
        )


def test_create_waiting_forever() -> None:
    aura_api = FakeAuraApi(status_after_creating="updating")
    _setup_db_instance(aura_api)
    sessions = AuraDsSessions(aura_api)

    with pytest.raises(RuntimeError, match="Failed to create session `one`: Instance is not running after waiting"):
        sessions.get_or_create(
            "one", SessionMemory.m_8GB, DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "", "")
        )


def test_invalid_session_name() -> None:
    aura_api = FakeAuraApi(status_after_creating="updating")
    _setup_db_instance(aura_api)
    sessions = AuraDsSessions(aura_api)

    with pytest.raises(ValueError, match="Session name `one_very_long_session_name` is too long. Max length is 18."):
        sessions.get_or_create(
            "one_very_long_session_name",
            SessionMemory.m_8GB,
            DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "", ""),
        )


def test_estimate_size() -> None:
    aura_api = FakeAuraApi(size_estimation=EstimationDetails("1GB", "8GB", False))
    sessions = AuraDsSessions(aura_api)

    assert sessions.estimate(1, 1, [AlgorithmCategory.CENTRALITY]) == SessionMemory.m_8GB


def test_estimate_size_exceeds() -> None:
    aura_api = FakeAuraApi(size_estimation=EstimationDetails("16GB", "8GB", True))
    sessions = AuraDsSessions(aura_api)

    with pytest.warns(
        ResourceWarning,
        match=re.escape("The estimated memory `16GB` exceeds the maximum size supported by your Aura tenant (`8GB`)"),
    ):
        assert sessions.estimate(1, 1, [AlgorithmCategory.CENTRALITY]) == SessionMemory.m_8GB


def _setup_db_instance(aura_api: AuraApi) -> None:
    aura_api.create_instance("test", "8GB", "aws", "leipzig-1")
