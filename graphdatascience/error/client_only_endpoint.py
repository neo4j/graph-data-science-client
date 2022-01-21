from typing import Any, Callable, Protocol, TypeVar, cast

F = TypeVar("F", bound=Callable[..., Any])


class WithNamespace(Protocol):
    _namespace: str


def client_only_endpoint(expected_namespace_prefix: str) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        def wrapper(self: WithNamespace, *args: Any, **kwargs: Any) -> Any:
            if self._namespace != expected_namespace_prefix:
                raise SyntaxError(
                    f"There is no '{self._namespace}.{func.__name__}' to call"
                )

            return func(self, *args, **kwargs)

        return cast(F, wrapper)

    return decorator
