from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple


@dataclass
class DbmsConnectionInfo:
    uri: str
    username: str
    password: str

    def auth(self) -> Tuple[str, str]:
        return self.username, self.password
