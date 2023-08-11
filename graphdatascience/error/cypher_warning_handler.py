import warnings
from functools import wraps
from typing import Any, Callable, TypeVar, cast

from ..caller_base import CallerBase

F = TypeVar("F", bound=Callable[..., Any])


def filter_id_func_deprecation_warning() -> Callable[[F], F]:
    def decorator(func: F) -> F:
        wraps(func)

        @wraps(func)
        def wrapper(self: CallerBase, *args: Any, **kwargs: Any) -> Any:
            # GDS uses the numeric id to resolve the node
            warnings.filterwarnings(
                "ignore",
                message=r"^The query used a deprecated function: `id`\.",
            )

            warnings.filterwarnings(
                "ignore",
                message=r"^The query used a deprecated function. \('id' is no longer supported\)",
            )

            return func(self, *args, **kwargs)

        return cast(F, wrapper)

    return decorator
