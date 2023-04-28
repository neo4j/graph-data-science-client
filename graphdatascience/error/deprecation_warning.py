from functools import wraps
from logging import warning
from typing import Any, Callable, Optional, TypeVar, cast

from ..server_version.server_version import ServerVersion

F = TypeVar("F", bound=Callable[..., Any])


class WithServerVersion:
    _server_version: ServerVersion


def deprecation_warning(
    new_procedure: str,
    deprecation_start_version: Optional[ServerVersion] = None,
) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        wraps(func)

        @wraps(func)
        def wrapper(self: WithServerVersion, *args: Any, **kwargs: Any) -> Any:
            if deprecation_start_version and self._server_version > deprecation_start_version:
                warning(f"Deprecated in favor of {new_procedure}")
            return func(self, *args, **kwargs)

        return cast(F, wrapper)

    return decorator
