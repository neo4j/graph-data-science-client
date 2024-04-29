from __future__ import annotations

import warnings
from typing import List, Optional

from neo4j import GraphDatabase

from graphdatascience.session.algorithm_category import AlgorithmCategory
from graphdatascience.session.aura_api import AuraApi
from graphdatascience.session.aura_api_responses import (
    InstanceCreateDetails,
    InstanceDetails,
    InstanceSpecificDetails,
)
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.session.region_suggester import closest_match
from graphdatascience.session.session_info import SessionInfo
from graphdatascience.session.session_sizes import SessionMemory


class AuraDsSessions:
    # Hardcoded neo4j user as sessions are always created with this user
    GDS_SESSION_USER = "neo4j"

    def __init__(self, aura_api: AuraApi) -> None:
        self._aura_api = aura_api

    def estimate(
        self, node_count: int, relationship_count: int, algorithm_categories: Optional[List[AlgorithmCategory]] = None
    ) -> SessionMemory:
        if algorithm_categories is None:
            algorithm_categories = []

        estimation = self._aura_api.estimate_size(node_count, relationship_count, algorithm_categories)

        if estimation.did_exceed_maximum:
            warnings.warn(
                f"The estimated memory `{estimation.min_required_memory}` exceeds the maximum size"
                f" supported by your Aura tenant (`{estimation.recommended_size}`).",
                ResourceWarning,
            )

        return SessionMemory(estimation.recommended_size)

    def get_or_create(
        self,
        session_name: str,
        memory: SessionMemory,
        db_connection: DbmsConnectionInfo,
    ) -> AuraGraphDataScience:
        existing_session = self._find_existing_session(session_name)

        if existing_session:
            session_id = existing_session.id
            # 0MB is AuraAPI default value for memory if none can be retrieved
            if existing_session.memory != "0MB" and existing_session.memory != memory.value:
                raise ValueError(
                    f"Session `{session_name}` already exists with memory `{existing_session.memory}`. "
                    f"Requested memory `{memory.value}` does not match."
                )
        else:
            create_details = self._create_session(session_name, memory, db_connection)
            session_id = create_details.id

        wait_result = self._aura_api.wait_for_instance_running(session_id)
        if err := wait_result.error:
            raise RuntimeError(f"Failed to create session `{session_name}`: {err}")

        gds_url = wait_result.connection_url

        if not existing_session:
            gds_user = create_details.username
            self._change_initial_pw(
                gds_url=gds_url, gds_user=gds_user, initial_pw=create_details.password, new_pw=db_connection.password
            )

        return self._construct_client(session_name=session_name, gds_url=gds_url, db_connection=db_connection)

    def delete(self, session_name: str) -> bool:
        instance_name = SessionNameHelper.instance_name(session_name)

        candidate_instances = [i for i in self._aura_api.list_instances() if i.name == instance_name]

        if len(candidate_instances) > 1:
            self._fail_ambiguous_session(session_name, candidate_instances)

        if len(candidate_instances) == 1:
            candidate = candidate_instances[0]
            return self._aura_api.delete_instance(candidate.id) is not None

        return False

    def list(self) -> List[SessionInfo]:
        all_instances = self._aura_api.list_instances()
        instance_details = [
            self._aura_api.list_instance(instance_id=instance.id)
            for instance in all_instances
            if SessionNameHelper.is_gds_session(instance)
        ]

        return [
            SessionInfo(SessionNameHelper.session_name(instance_detail.name), instance_detail.memory)
            for instance_detail in instance_details
            if instance_detail
        ]

    def _find_existing_session(self, session_name: str) -> Optional[InstanceSpecificDetails]:
        instance_name = SessionNameHelper.instance_name(session_name)
        matched_instances = [instance for instance in self._aura_api.list_instances() if instance.name == instance_name]

        if len(matched_instances) == 0:
            return None

        if len(matched_instances) > 1:
            self._fail_ambiguous_session(session_name, matched_instances)

        return self._aura_api.list_instance(matched_instances[0].id)

    def _create_session(
        self, session_name: str, memory: SessionMemory, db_connection: DbmsConnectionInfo
    ) -> InstanceCreateDetails:
        db_instance_id = AuraApi.extract_id(db_connection.uri)
        db_instance = self._aura_api.list_instance(db_instance_id)
        if not db_instance:
            raise ValueError(f"Could not find AuraDB instance with the uri `{db_connection.uri}`")

        region = self._ds_region(db_instance.region, db_instance.cloud_provider)

        create_details = self._aura_api.create_instance(
            SessionNameHelper.instance_name(session_name), memory.value, db_instance.cloud_provider, region
        )
        return create_details

    def _ds_region(self, region: str, cloud_provider: str) -> str:
        tenant_details = self._aura_api.tenant_details()
        available_regions = tenant_details.regions_per_provider[cloud_provider]

        match = closest_match(region, available_regions)
        if not match:
            raise ValueError(
                f"Tenant `{tenant_details.id}` cannot create GDS sessions at cloud provider `{cloud_provider}`."
            )

        return match

    def _construct_client(
        self, session_name: str, gds_url: str, db_connection: DbmsConnectionInfo
    ) -> AuraGraphDataScience:
        return AuraGraphDataScience(
            gds_session_connection_info=DbmsConnectionInfo(
                gds_url, AuraDsSessions.GDS_SESSION_USER, db_connection.password
            ),
            aura_db_connection_info=db_connection,
            delete_fn=lambda: self.delete(session_name),
        )

    @staticmethod
    def _change_initial_pw(gds_url: str, gds_user: str, initial_pw: str, new_pw: str) -> None:
        with GraphDatabase.driver(gds_url, auth=(gds_user, initial_pw)) as driver:
            with driver.session(database="system") as session:
                session.run(
                    "ALTER CURRENT USER SET PASSWORD FROM $old_pw TO $new_pw", {"old_pw": initial_pw, "new_pw": new_pw}
                )

    @classmethod
    def _fail_ambiguous_session(cls, session_name: str, instances: List[InstanceDetails]) -> None:
        candidates = [(i.id, SessionNameHelper.session_name(i.name)) for i in instances]
        raise RuntimeError(
            f"Expected to find exactly one GDS session with name `{session_name}`, but found `{candidates}`."
        )


class SessionNameHelper:
    GDS_SESSION_NAME_PREFIX = "gds-session-"
    MAX_INSTANCE_NAME_LENGTH = 30

    @classmethod
    def session_name(cls, instance_name: str) -> str:
        # str.removeprefix is only available in Python 3.9+
        if instance_name.startswith(cls.GDS_SESSION_NAME_PREFIX):
            return instance_name[len(cls.GDS_SESSION_NAME_PREFIX) :]  # noqa: E203 (black vs flake8 conflict)
        else:
            raise ValueError(
                f"Invalid session name: `{instance_name}`. "
                f"The name must begin with the prefix `{SessionNameHelper.GDS_SESSION_NAME_PREFIX}`"
            )

    @classmethod
    def instance_name(cls, session_name: str) -> str:
        prefix_len = len(cls.GDS_SESSION_NAME_PREFIX)
        if prefix_len + len(session_name) > cls.MAX_INSTANCE_NAME_LENGTH:
            raise ValueError(
                f"Session name `{session_name}` is too long."
                f" Max length is {cls.MAX_INSTANCE_NAME_LENGTH - prefix_len}."
            )

        return f"{cls.GDS_SESSION_NAME_PREFIX}{session_name}"

    @classmethod
    def is_gds_session(cls, instance: InstanceDetails) -> bool:
        return instance.name.startswith(cls.GDS_SESSION_NAME_PREFIX)
