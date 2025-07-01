from typing import Optional, Any, Dict

from graphdatascience import Graph


class ArrowConfigConverter:

    @staticmethod
    def build_configuration(G: Graph, **kwargs: Optional[Any]) -> dict[str, Any]:
        config: dict[str, Any] = {
            "graphName": G.name(),
        }

        # Process kwargs
        processed_kwargs = ArrowConfigConverter._process_dict_values(kwargs)
        config.update(processed_kwargs)

        return config

    @staticmethod
    def _convert_to_camel_case(name: str) -> str:
        """Convert a snake_case string to camelCase."""
        parts = name.split('_')
        return ''.join([word.capitalize() if i > 0 else word.lower() for i, word in enumerate(parts)])

    @staticmethod
    def _process_dict_values(input_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Process dictionary values, converting keys to camelCase and handling nested dictionaries."""
        result = {}
        for key, value in input_dict.items():
            if value is not None:
                camel_key = ArrowConfigConverter._convert_to_camel_case(key)
                # Recursively process nested dictionaries
                if isinstance(value, dict):
                    result[camel_key] = ArrowConfigConverter._process_dict_values(value)
                else:
                    result[camel_key] = value
        return result