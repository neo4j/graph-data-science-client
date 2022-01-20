from abc import ABC
from typing import Any, NoReturn


class UncallableNamespace(ABC):
    def __init__(self, namespace: str):
        self._namespace = namespace

    def __call__(self, *_: Any, **__: Any) -> NoReturn:
        raise SyntaxError(f"There is no '{self._namespace}' to call")
