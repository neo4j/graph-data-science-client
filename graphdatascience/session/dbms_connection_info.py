from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple


@dataclass
class DbmsConnectionInfo:
    """
    Represents the connection information for a DBMS.
    """

    uri: str
    username: str
    password: str

    def auth(self) -> Tuple[str, str]:
        """
        Returns the username and password for authentication.
        
        Returns:
            A tuple containing the username and password.
        """
        return self.username, self.password
