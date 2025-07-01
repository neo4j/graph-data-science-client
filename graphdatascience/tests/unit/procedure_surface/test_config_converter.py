from graphdatascience.procedure_surface.config_converter import ConfigConverter


def test_build_configuration_with_no_additional_args() -> None:
    config = ConfigConverter.convert_to_gds_config()
    assert config == {}


def test_build_configuration_with_additional_args() -> None:
    config = ConfigConverter.convert_to_gds_config(some_property="value", another_property=42)
    assert config["someProperty"] == "value"
    assert config["anotherProperty"] == 42


def test_build_configuration_ignores_none_values() -> None:
    config = ConfigConverter.convert_to_gds_config(included_property="present", excluded_property=None)
    assert "includedProperty" in config
    assert "excludedProperty" not in config
    assert config["includedProperty"] == "present"


def test_build_configuration_with_nested_dict() -> None:
    config = ConfigConverter.convert_to_gds_config(foo_bar={"bar_baz": 42, "another_key": "value"})
    assert "fooBar" in config
    assert isinstance(config["fooBar"], dict)
    assert "barBaz" in config["fooBar"]
    assert config["fooBar"]["barBaz"] == 42
    assert "anotherKey" in config["fooBar"]
    assert config["fooBar"]["anotherKey"] == "value"


def test_build_configuration_with_deeply_nested_dict() -> None:
    config = ConfigConverter.convert_to_gds_config(level_one={"level_two": {"level_three": 42}})
    assert "levelOne" in config
    assert isinstance(config["levelOne"], dict)
    assert "levelTwo" in config["levelOne"]
    assert isinstance(config["levelOne"]["levelTwo"], dict)
    assert "levelThree" in config["levelOne"]["levelTwo"]
    assert config["levelOne"]["levelTwo"]["levelThree"] == 42
