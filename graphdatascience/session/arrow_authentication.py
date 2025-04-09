from abc import ABC, abstractmethod
from typing import Callable

from graphdatascience.session.aura_api import AuraApi


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


class AuraApiTokenAuthentication(ArrowAuthentication):
    def __init__(self, aura_api: AuraApi):
        self._aura_api = aura_api

    def auth_pair(self) -> tuple[str, str]:
        return "", self._aura_api._auth._auth_token()
