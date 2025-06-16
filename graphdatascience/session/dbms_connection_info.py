from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from neo4j import Auth, basic_auth


@dataclass
class DbmsConnectionInfo:
    """
    Represents the connection information for a Neo4j DBMS, such as an AuraDB instance.
    Supports both username/password as well as the authentication options provided by the Neo4j Python driver.
    """

    uri: str
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    # Optional: typed authentication, used instead of username/password. Supports for example a token. See https://neo4j.com/docs/python-manual/current/connect-advanced/#authentication-methods
    auth: Optional[Auth] = None

    def __post_init__(self) -> None:
        # Validate auth fields
        if (self.username or self.password) and self.auth:
            raise ValueError(
                "Cannot provide both username/password and token for authentication. "
                "Please provide either a username/password or a token."
            )

    def get_auth(self) -> Optional[Auth]:
        """
        Returns:
            A neo4j.Auth object for authentication.
        """
        auth = self.auth
        if self.username and self.password:
            auth = basic_auth(self.username, self.password)
        return auth

    @staticmethod
    def from_env() -> DbmsConnectionInfo:
        """
        Create a DbmsConnectionInfo instance from environment variables.
        The environment variables are:
        - NEO4J_URI
        - NEO4J_USERNAME
        - NEO4J_PASSWORD
        - NEO4J_DATABASE
        """
        uri = os.environ["NEO4J_URI"]
        username = os.environ.get("NEO4J_USERNAME", "neo4j")
        password = os.environ["NEO4J_PASSWORD"]
        database = os.environ.get("NEO4J_DATABASE")

        return DbmsConnectionInfo(uri, username, password, database)
