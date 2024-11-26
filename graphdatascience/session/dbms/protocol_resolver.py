from typing import Optional

from neo4j.exceptions import Neo4jError

from graphdatascience import QueryRunner
from graphdatascience.session.dbms.protocol_version import ProtocolVersion


class ProtocolVersionResolver:
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def resolve(self) -> ProtocolVersion:
        """
        Resolve the protocol versions supported by the DBMS instance with the protocol versions supported by this client
        Returns the latest protocol version supported by both.
        """

        server_versions = self._protocol_versions_from_server()
        if server_versions:
            return server_versions[0]

        raise UnsupportedProtocolVersion(
            """
            The GDS Python Client does not support any procedure protocol version in the server.
            Please update the GDS Python Client to the newest version.
            """.strip()
        )

    def _protocol_versions_from_server(self) -> list[ProtocolVersion]:
        cached_protocol_versions = self._fetch_from_server()
        cached_protocol_versions.sort(reverse=True, key=lambda x: x.value)

        return cached_protocol_versions

    def _fetch_from_server(self) -> list[ProtocolVersion]:
        try:
            version_list = []
            for version_string in self._query_runner.call_procedure(
                "gds.session.dbms.protocol.version", yields=["version"]
            )["version"].to_list():
                parsed_version = self._from_str(version_string)
                if parsed_version:
                    version_list.append(parsed_version)

            return version_list

        except Neo4jError:
            return [ProtocolVersion.V1]

    @staticmethod
    def _from_str(version_string: str) -> Optional[ProtocolVersion]:
        if version_string == "v1":
            return ProtocolVersion.V1
        elif version_string == "v2":
            return ProtocolVersion.V2
        elif version_string == "v3":
            return ProtocolVersion.V3
        else:
            return None


class UnsupportedProtocolVersion(Exception):
    pass
