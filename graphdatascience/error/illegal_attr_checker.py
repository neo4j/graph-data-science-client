from abc import ABC
from typing import NoReturn

from ..caller_base import CallerBase


class IllegalAttrChecker(CallerBase, ABC):
    def __getattr__(self, attr: str) -> NoReturn:
        self._raise_suggestive_error_message(f"{self._namespace}.{attr}")
