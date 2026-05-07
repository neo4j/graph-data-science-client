def test_convert_to_parameter_space_config_is_available_from_shared_pipeline_module() -> None:
    from graphdatascience.procedure_surface.api.pipeline.parameter_space_config import (
        convert_to_parameter_space_config,
    )

    assert convert_to_parameter_space_config(
        range_keys={"penalty"},
        model_type="LinearRegression",
        penalty=(0.1, 0.5),
    ) == {"modelType": "LinearRegression", "penalty": {"range": [0.1, 0.5]}}
