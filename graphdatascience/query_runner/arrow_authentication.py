from abc import ABC, abstractmethod


class ArrowAuthentication(ABC):
    @abstractmethod
    def auth_pair(self) -> tuple[str, str]:
        """Returns the auth pair used for authentication."""
        pass


class UsernamePasswordAuthentication(ArrowAuthentication):
    def __init__(self, username: str, password: str):
        self._username = username
        self._password = password

    def auth_pair(self) -> tuple[str, str]:
        return self._username, self._password
