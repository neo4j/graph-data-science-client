from abc import ABC
from typing import Any, NoReturn

from ..caller_base import CallerBase


class UncallableNamespace(CallerBase, ABC):
    def __call__(self, *_: Any, **__: Any) -> NoReturn:
        self._raise_suggestive_error_message(self._namespace)
