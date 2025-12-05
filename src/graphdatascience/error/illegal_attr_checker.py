from abc import ABC
from typing import NoReturn

from ..caller_base import CallerBase
from ..ignored_server_endpoints import IGNORED_SERVER_ENDPOINTS


class IllegalAttrChecker(CallerBase, ABC):
    def __getattr__(self, attr: str) -> NoReturn:
        requested_endpoint = f"{self._namespace}.{attr}"
        if requested_endpoint in IGNORED_SERVER_ENDPOINTS:
            raise SyntaxError(
                f"The call '{requested_endpoint}' is a valid GDS server endpoint, "
                "but does not have a corresponding Python method"
            )
        self._raise_suggestive_error_message(f"{self._namespace}.{attr}")
