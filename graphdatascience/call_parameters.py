from typing import Any, OrderedDict


class CallParameters(OrderedDict[str, Any]):
    # since Python 3.6 also initializing through CallParameters(**kwargs) is order preserving

    def placeholder_str(self) -> str:
        return ", ".join([f"${k}" for k in self.keys()])
