from abc import ABC
from functools import wraps
from typing import Any, Callable, TypeVar, cast

F = TypeVar("F", bound=Callable[..., Any])


class WithNamespace(ABC):
    _namespace: str


def client_only_endpoint(expected_namespace_prefix: str) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        wraps(func)

        def wrapper(self: WithNamespace, *args: Any, **kwargs: Any) -> Any:
            if self._namespace != expected_namespace_prefix:
                raise SyntaxError(f"There is no '{self._namespace}.{func.__name__}' to call")

            return func(self, *args, **kwargs)

        return cast(F, wrapper)

    return decorator
