from abc import ABC, abstractmethod

from graphdatascience.session.aura_api import AuraApi


class LifecycleManager(ABC):
    @abstractmethod
    def delete(self) -> bool:
        pass

    @abstractmethod
    def verify_health(self) -> None:
        # Raises a SessionStatusError if the season is in an unhealthy state
        pass


class SessionLifecycleManager(LifecycleManager):
    def __init__(self, session_id: str, aura_api: AuraApi):
        self.session_id = session_id
        self._aura_api = aura_api

    def delete(self) -> bool:
        return self._aura_api.delete_session(self.session_id)

    def verify_health(self) -> None:
        # Raises a SessionStatusError if the season is in an unhealthy state
        self._aura_api.get_session(self.session_id)


class Noop(LifecycleManager):
    def delete(self) -> bool:
        return True

    def verify_health(self) -> None:
        pass
