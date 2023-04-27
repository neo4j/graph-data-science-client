from functools import wraps
from logging import warning
from typing import Any, Callable, TypeVar, cast

from ..caller_base import CallerBase

F = TypeVar("F", bound=Callable[..., Any])


def deprecation_warning(new_procedure: str) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        wraps(func)

        @wraps(func)
        def wrapper(self: CallerBase, *args: Any, **kwargs: Any) -> Any:
            warning(f"Deprecated in favor of {new_procedure}")
            return func(self, *args, **kwargs)

        return cast(F, wrapper)

    return decorator
