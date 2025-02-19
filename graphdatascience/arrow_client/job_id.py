from dataclasses import dataclass

@dataclass(repr=True, frozen=True)
class JobId:
    id: str

    @staticmethod
    def from_json(json: dict[str, str]):
        return JobId(id=json["jobId"])

    def to_json(self) -> dict[str, str]:
        return {"jobId": self.id}