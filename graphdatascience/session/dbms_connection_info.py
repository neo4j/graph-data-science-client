from __future__ import annotations

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
