from abc import ABC, abstractmethod
from dataclasses import dataclass


class ArrowAuthentication(ABC):
    @abstractmethod
    def auth_pair(self) -> tuple[str, str]:
        """Returns the auth pair used for authentication."""
        pass


@dataclass
class UsernamePasswordAuthentication(ArrowAuthentication):
    username: str
    password: str

    def auth_pair(self) -> tuple[str, str]:
        return self.username, self.password
