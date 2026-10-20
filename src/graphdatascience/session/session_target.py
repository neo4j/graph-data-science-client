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

    A `None` `instance_id` means the session is self-managed, i.e. not attached to any DBMS.
    """

    db_runner: Neo4jQueryRunner | None
    instance_id: str | None = None
    instance_type: InstanceType = InstanceType.DATABASE
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
            return SessionTarget(db_runner=None)

        instance_id = db_connection.aura_instance_id
        aura_db_instance = self._aura_api.list_instance(instance_id) if instance_id else None

        if aura_db_instance:
            # an AuraDB instance provides its own uri
            db_connection.set_uri(aura_db_instance.connection_url)
        elif instance_id and not db_connection.uri:
            # the id is not an AuraDB instance and there is no uri to reach a virtual graph or composite
            raise _instance_not_found_error(instance_id)

        db_runner = self._create_db_runner(db_connection, driver_config)
        instance_type = InstanceTypeResolver(db_runner).resolve()
        self._logger.debug(f"Detected instance type `{instance_type}` for `{db_connection.uri or instance_id}`.")
        database_id = db_connection.aura_database_id

        if aura_db_instance:
            db_runner.hosted_in_aura = True
            return SessionTarget(db_runner, aura_db_instance.id, instance_type, database_id)

        if instance_type is not InstanceType.DATABASE:
            db_runner.hosted_in_aura = True
            if instance_id:
                return SessionTarget(db_runner, instance_id, instance_type, database_id)

            warnings.warn(DeprecationWarning(_DERIVE_FROM_URI_DEPRECATION))
            return SessionTarget(db_runner, AuraApi.extract_id(db_connection.get_uri()), instance_type, database_id)

        if instance_id:
            # the id is neither an AuraDB instance nor a virtual graph or composite
            raise _instance_not_found_error(instance_id)

        if db_runner.resolve_hosted_in_aura():
            warnings.warn(DeprecationWarning(_DERIVE_FROM_URI_DEPRECATION))
            derived_id = AuraApi.extract_id(db_connection.get_uri())
            derived_instance = self._aura_api.list_instance(derived_id)
            if not derived_instance:
                raise ValueError(
                    f"Aura instance with id `{derived_id}` could not be found. Please specify the `aura_instance_id` in the `db_connection` argument."
                )
            return SessionTarget(db_runner, derived_instance.id, instance_type, database_id)

        return SessionTarget(db_runner)


def _instance_not_found_error(instance_id: str) -> ValueError:
    return ValueError(
        f"Aura instance with id `{instance_id}` could not be found. "
        "Please verify that the instance id is correct and that you have access to the Aura instance. "
        "If it is a virtual graph or composite instance, provide its `uri`."
    )
