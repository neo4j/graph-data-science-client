from enum import Enum


class ProtocolVersion(Enum):
    V1 = "v1"
    V2 = "v2"
    V3 = "v3"

    def versioned_procedure_name(self, procedure_name: str) -> str:
        if self == ProtocolVersion.V1:
            return procedure_name
        else:
            return f"{procedure_name}.{self.value}"
