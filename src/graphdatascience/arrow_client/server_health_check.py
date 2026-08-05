from typing import Protocol


class ServerHealthCheck(Protocol):
    def raise_if_unhealthy(self) -> None:
        """
        Raises a descriptive error if the server is known to be in an unhealthy state,
        and returns without raising if the state could not be determined.
        """
        ...
