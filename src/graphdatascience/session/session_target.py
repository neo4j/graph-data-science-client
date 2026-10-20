from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass
from typing import Any, Callable

from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.session.aura_api import AuraApi
from graphdatascience.session.dbms.instance_type_resolver import InstanceTypeResolver
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.session.instance_type import InstanceType

_DERIVE_FROM_URI_DEPRECATION = (
    "Deriving the Aura instance from the database URI is deprecated and will be removed in a future release. "
    "Please specify the `aura_instance_id` in the `db_connection` argument."
)


@dataclass
class SessionTarget:
    """
    The DBMS a session attaches to.

    A `None` `instance_type` and `instance_id` mean the session is self-managed, i.e. not attached to any DBMS.
    """

    db_runner: Neo4jQueryRunner | None
    instance_type: InstanceType | None
    instance_id: str | None = None
    database_id: str | None = None


class SessionTargetResolver:
    """
    Connects to the DBMS a session should attach to and detects its instance type.
    """

    def __init__(
        self,
        aura_api: AuraApi,
        create_db_runner: Callable[[DbmsConnectionInfo, dict[str, Any] | None], Neo4jQueryRunner],
    ) -> None:
        self._aura_api = aura_api
        self._create_db_runner = create_db_runner
        self._logger = logging.getLogger(__name__)

    def resolve(self, db_connection: DbmsConnectionInfo | None, driver_config: dict[str, Any] | None) -> SessionTarget:
        if db_connection is None:
            return SessionTarget(db_runner=None, instance_type=None)

        instance_id = db_connection.aura_instance_id
        database_id = db_connection.aura_database_id

        if db_connection.uri:
            # With a uri we can connect right away, so detect the type before involving the Aura API.
            db_runner = self._create_db_runner(db_connection, driver_config)
            instance_type = self._detect_instance_type(db_runner, db_connection)

            if instance_type is not InstanceType.DATABASE:
                # virtual graphs and composites are only reachable through their own uri
                db_runner.hosted_in_aura = True
                if instance_id:
                    return SessionTarget(
                        db_runner=db_runner,
                        instance_type=instance_type,
                        instance_id=instance_id,
                        database_id=database_id,
                    )

                warnings.warn(DeprecationWarning(_DERIVE_FROM_URI_DEPRECATION))
                return SessionTarget(
                    db_runner=db_runner,
                    instance_type=instance_type,
                    instance_id=AuraApi.extract_id(db_connection.get_uri()),
                    database_id=database_id,
                )

            if instance_id:
                # an AuraDB instance is the only type the Aura API can resolve
                aura_db_instance = self._aura_api.list_instance(instance_id)
                if not aura_db_instance:
                    raise _instance_not_found_error(instance_id)

                db_runner.hosted_in_aura = True
                return SessionTarget(
                    db_runner=db_runner,
                    instance_type=instance_type,
                    instance_id=aura_db_instance.id,
                    database_id=database_id,
                )

            if db_runner.resolve_hosted_in_aura():
                warnings.warn(DeprecationWarning(_DERIVE_FROM_URI_DEPRECATION))
                derived_id = AuraApi.extract_id(db_connection.get_uri())
                derived_instance = self._aura_api.list_instance(derived_id)
                if not derived_instance:
                    raise ValueError(
                        f"Aura instance with id `{derived_id}` could not be found. Please specify the `aura_instance_id` in the `db_connection` argument."
                    )
                return SessionTarget(
                    db_runner=db_runner,
                    instance_type=instance_type,
                    instance_id=derived_instance.id,
                    database_id=database_id,
                )

            return SessionTarget(db_runner=db_runner, instance_type=None)

        # Without a uri the Aura API has to provide it, so the id must be an AuraDB instance.
        if not instance_id:
            raise ValueError("Either 'uri' or 'aura_instance_id' must be provided.")

        aura_db_instance = self._aura_api.list_instance(instance_id)
        if not aura_db_instance:
            raise _instance_not_found_error(instance_id)

        db_connection.set_uri(aura_db_instance.connection_url)

        db_runner = self._create_db_runner(db_connection, driver_config)
        db_runner.hosted_in_aura = True
        instance_type = self._detect_instance_type(db_runner, db_connection)
        return SessionTarget(
            db_runner=db_runner,
            instance_type=instance_type,
            instance_id=aura_db_instance.id,
            database_id=database_id,
        )

    def _detect_instance_type(self, db_runner: Neo4jQueryRunner, db_connection: DbmsConnectionInfo) -> InstanceType:
        instance_type = InstanceTypeResolver(db_runner).resolve()
        self._logger.debug(f"Detected instance type `{instance_type}` for `{db_connection.get_uri()}`.")
        return instance_type


def _instance_not_found_error(instance_id: str) -> ValueError:
    return ValueError(
        f"Aura instance with id `{instance_id}` could not be found. "
        "Please verify that the instance id is correct and that you have access to the Aura instance. "
        "If it is a virtual graph or composite instance, provide its `uri`."
    )
