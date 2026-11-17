from __future__ import annotations

from typing import overload


@overload
def as_list(value: str) -> list[str]: ...


@overload
def as_list(value: list[str]) -> list[str]: ...


@overload
def as_list(value: None) -> None: ...


def as_list(value: str | list[str] | None) -> list[str] | None:
    # user-facing args accept a single string; internals want a list
    if isinstance(value, str):
        return [value]
    return value
