from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class DbmsConnectionInfo:
    """
    Represents the connection information for a Neo4j DBMS, such as an AuraDB instance.
    """

    uri: str
    username: str
    password: str
    database: Optional[str] = None

    def auth(self) -> tuple[str, str]:
        """
        Returns the username and password for authentication.

        Returns:
            A tuple containing the username and password.
        """
        return self.username, self.password

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
