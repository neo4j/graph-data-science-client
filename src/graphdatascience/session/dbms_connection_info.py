from __future__ import annotations

import os
from dataclasses import dataclass

from neo4j import Auth, basic_auth


@dataclass
class DbmsConnectionInfo:
    """
    Represents the connection information for a Neo4j DBMS, such as an AuraDB instance.
    Supports both username/password as well as the authentication options provided by the Neo4j Python driver.
    """

    # 'uri' or 'aura_instance_id' must be provided.
    uri: str | None = None

    username: str | None = None
    password: str | None = None
    database: str | None = None
    # Optional: typed authentication, used instead of username/password. Supports for example a token. See https://neo4j.com/docs/python-manual/current/connect-advanced/#authentication-methods
    auth: Auth | None = None

    aura_instance_id: str | None = None

    def __post_init__(self) -> None:
        # Validate auth fields
        if (self.username or self.password) and self.auth:
            raise ValueError(
                "Cannot provide both username/password and token for authentication. "
                "Please provide either a username/password or a token."
            )

        if (self.aura_instance_id is None) and (self.uri is None):
            raise ValueError("Either 'uri' or 'aura_instance_id' must be provided.")

    def get_auth(self) -> Auth | None:
        """
        Returns:
            A neo4j.Auth object for authentication.
        """
        auth = self.auth
        if self.username and self.password:
            auth = basic_auth(self.username, self.password)
        return auth

    def set_uri(self, uri: str) -> None:
        self.uri = uri

    def get_uri(self) -> str:
        if not self.uri:
            raise ValueError("'uri' is not provided.")
        return self.uri

    @staticmethod
    def from_env() -> DbmsConnectionInfo:
        """
        Create a DbmsConnectionInfo instance from environment variables.
        The environment variables are:
        - NEO4J_URI
        - NEO4J_USERNAME
        - NEO4J_PASSWORD
        - NEO4J_DATABASE
        - AURA_INSTANCEID
        """
        username = os.environ.get("NEO4J_USERNAME", "neo4j")
        password = os.environ["NEO4J_PASSWORD"]
        database = os.environ.get("NEO4J_DATABASE")
        aura_instance_id = os.environ.get("AURA_INSTANCEID")

        # instance id takes precedence over uri
        if not aura_instance_id:
            uri = os.environ["NEO4J_URI"]
        else:
            uri = None

        return DbmsConnectionInfo(uri, username, password, database, aura_instance_id=aura_instance_id)
