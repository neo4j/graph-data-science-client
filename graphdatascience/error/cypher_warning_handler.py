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

            # previously      The query used a deprecated function. ('id' is no longer supported)
            # since 2025.04.0 The query used a deprecated function. ('id' has been replaced by 'elementId or an application-generated id')
            # since 2025.06   The query used a deprecated function. ('id' has been replaced by 'elementId or consider using an application-generated id')
            warnings.filterwarnings(
                "ignore",
                message=r"The query used a deprecated function. \('id'.*",
            )

            return func(self, *args, **kwargs)

        return cast(F, wrapper)

    return decorator
