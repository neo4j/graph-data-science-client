from abc import ABC
from typing import Any, NoReturn

from ..caller_base import CallerBase
from ..ignored_server_endpoints import IGNORED_SERVER_ENDPOINTS


class UncallableNamespace(CallerBase, ABC):
    def __call__(self, *_: Any, **__: Any) -> NoReturn:
        if self._namespace in IGNORED_SERVER_ENDPOINTS:
            raise SyntaxError(
                f"The call '{self._namespace}' is a valid GDS server endpoint, "
                "but does not have a corresponding Python method"
            )

        self._raise_suggestive_error_message(self._namespace)
