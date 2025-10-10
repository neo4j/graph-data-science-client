from typing import Any


class ConfigConverter:
    @staticmethod
    def convert_to_gds_config(**kwargs: Any | None) -> dict[str, Any]:
        config: dict[str, Any] = {}

        # Process kwargs
        processed_kwargs = ConfigConverter._process_dict_values(kwargs)
        config.update(processed_kwargs)

        return config

    @staticmethod
    def _convert_to_camel_case(name: str) -> str:
        """Convert a snake_case string to camelCase."""
        parts = name.split("_")

        # skip if already converted
        if len(parts) == 1:
            return name

        return "".join([word.capitalize() if i > 0 else word.lower() for i, word in enumerate(parts)])

    @staticmethod
    def _process_dict_values(input_dict: dict[str, Any]) -> dict[str, Any]:
        """Process dictionary values, converting keys to camelCase and handling nested dictionaries."""
        result = {}
        for key, value in input_dict.items():
            if value is not None:
                camel_key = ConfigConverter._convert_to_camel_case(key)
                # Recursively process nested dictionaries
                if isinstance(value, dict):
                    result[camel_key] = ConfigConverter._process_dict_values(value)
                else:
                    result[camel_key] = value
        return result
