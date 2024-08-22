from typing import List

from neo4j.exceptions import Neo4jError

from graphdatascience import QueryRunner
from graphdatascience.session.dbms.protocol_version import (
    ProtocolVersion,
    ProtocolVersions,
)


class ProtocolVersionResolver:

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner
        self._cached_protocol_versions: List[ProtocolVersion] = []

    def protocol_versions_from_server(self) -> List[ProtocolVersion]:
        """
        Get the protocol versions supported by the AuraDB instance.
        Returns 'v1' if the procedure was not found, which indicates an older version of the database.
        """

        if not self._cached_protocol_versions:
            self._cached_protocol_versions = self._fetch_from_server()

        return self._cached_protocol_versions

    def _fetch_from_server(self) -> List[ProtocolVersion]:
        try:
            return [
                ProtocolVersions.from_str(version_string)
                for version_string in (
                    self._query_runner.call_procedure("gds.session.dbms.protocol.version", yields=["version"])[
                        "version"
                    ].to_list()
                )
            ]
        except Neo4jError:
            return [ProtocolVersion.V1]
