from __future__ import annotations

from typing import Any

from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter


def convert_to_parameter_space_config(*, range_keys: set[str], **kwargs: Any | None) -> dict[str, Any]:
    config = {
        key: _to_parameter_space_value(key, value) if key in range_keys else value for key, value in kwargs.items()
    }

    return ConfigConverter.convert_to_gds_config(**config)


def _to_parameter_space_value(key: str, value: Any) -> Any:
    if isinstance(value, tuple):
        if len(value) != 2:
            raise ValueError(f"{key} range inputs must be tuples with exactly two values.")
        return {"range": list(value)}

    if isinstance(value, list):
        raise ValueError(f"{key} range inputs must be tuples with exactly two values.")

    return value
