from functools import wraps
from inspect import signature
from typing import Any, Callable, Optional, TypeVar, cast

from .server_version import ServerVersion

F = TypeVar("F", bound=Callable[..., Any])


class IncompatibleServerVersionError(Exception):
    pass


class WithNamespaceAndServerVersion:
    _namespace: str
    _server_version: ServerVersion


def compatible_with(
    func_name: str, min_inclusive: Optional[ServerVersion] = None, max_exclusive: Optional[ServerVersion] = None
) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(self: WithNamespaceAndServerVersion, *args: Any, **kwargs: Any) -> Any:
            parameters = list(signature(func).parameters)[1:]

            if min_inclusive and self._server_version < min_inclusive:
                raise IncompatibleServerVersionError(
                    f"The call {self._namespace}.{func_name} with parameters {parameters} requires GDS server "
                    f"version >= {min_inclusive}. The current version is {self._server_version}"
                )

            if max_exclusive and self._server_version >= max_exclusive:
                raise IncompatibleServerVersionError(
                    f"The call {self._namespace}.{func_name} with parameters {parameters} requires GDS server "
                    f"version < {max_exclusive}. The current version is {self._server_version}"
                )

            return func(self, *args, **kwargs)

        return cast(F, wrapper)

    return decorator
