from graphdatascience.session import SessionMemory


def test_all_values() -> None:
    assert len(SessionMemory.all_values()) == 14
    for e in SessionMemory.all_values():
        assert e.value.endswith("GB")
