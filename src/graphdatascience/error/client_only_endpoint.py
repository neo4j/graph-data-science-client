import warnings
from functools import wraps
from typing import Any, Callable, TypeVar, cast

from ..caller_base import CallerBase

F = TypeVar("F", bound=Callable[..., Any])


def client_only_endpoint(expected_namespace_prefix: str) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        wraps(func)

        @wraps(func)
        def wrapper(self: CallerBase, *args: Any, **kwargs: Any) -> Any:
            if self._namespace != expected_namespace_prefix:
                raise SyntaxError(
                    f"There is no '{self._namespace}.{func.__name__}' to call. "
                    f"Did you mean '{expected_namespace_prefix}.{func.__name__}?"
                )

            return func(self, *args, **kwargs)

        return cast(F, wrapper)

    return decorator


def client_deprecated(
    old_endpoint: str,
    new_endpoint: str,
) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        wraps(func)

        @wraps(func)
        def wrapper(self: CallerBase, *args: Any, **kwargs: Any) -> Any:
            warnings.warn(f"Deprecated `{old_endpoint}` in favor of `{new_endpoint}`", DeprecationWarning)
            return func(self, *args, **kwargs)

        return cast(F, wrapper)

    return decorator
