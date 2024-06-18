from graphdatascience.session import SessionMemory


def test_all_values() -> None:
    assert len(SessionMemory.all_values()) == 12
    for e in SessionMemory.all_values():
        assert e.value.endswith("GB")
