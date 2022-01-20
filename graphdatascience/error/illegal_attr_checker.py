from abc import ABC
from typing import NoReturn


class IllegalAttrChecker(ABC):
    def __init__(self, namespace: str):
        self._namespace = namespace

    def __getattr__(self, attr: str) -> NoReturn:
        raise SyntaxError(f"There is no '{self._namespace}.{attr}' to call")
