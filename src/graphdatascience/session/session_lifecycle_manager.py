from graphdatascience.session.aura_api import AuraApi


class SessionLifecycleManager:
    def __init__(self, session_id: str, aura_api: AuraApi):
        self.session_id = session_id
        self._aura_api = aura_api

    def delete(self) -> bool:
        return self._aura_api.delete_session(self.session_id)

    def verify_health(self) -> None:
        # Raises a SessionStatusError if the season is in an unhealthy state
        self._aura_api.get_session(self.session_id)
