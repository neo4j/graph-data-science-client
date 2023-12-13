from enum import Enum
from typing import Type


class SessionSizeByMemory(Enum):
    XS = ("1GB",)
    S = ("2GB",)
    SM = ("4GB",)
    M = ("8GB",)
    ML = ("16GB",)
    L = ("32GB",)
    XL = ("64GB",)
    XXL = ("128GB",)
    XXXL = ("256GB",)
    XXXXL = ("320GB",)
    XXXXXL = "512GB"


class SessionSizes:
    @staticmethod
    def by_memory() -> Type[SessionSizeByMemory]:
        return SessionSizeByMemory
