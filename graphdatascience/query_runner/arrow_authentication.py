from abc import ABC, abstractmethod
from typing import Callable


class ArrowAuthentication(ABC):
    type AuthTokenFn = Callable[[], str]

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
