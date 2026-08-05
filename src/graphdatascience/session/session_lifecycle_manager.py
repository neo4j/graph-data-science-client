import logging
from abc import ABC, abstractmethod

from graphdatascience.session.aura_api import AuraApi, SessionStatusError


class LifecycleManager(ABC):
    @abstractmethod
    def delete(self) -> bool:
        pass

    @abstractmethod
    def verify_health(self) -> None:
        # Raises a SessionStatusError if the session is in an unhealthy state
        pass

    def raise_if_unhealthy(self) -> None:
        """
        Best effort health check to explain a failure that was already observed elsewhere.

        Raises a SessionStatusError if the session is known to be in an unhealthy state.
        Any other failure (such as the Aura API being unreachable) is only logged.
        """
        try:
            self.verify_health()
        except SessionStatusError:
            raise
        except Exception as e:
            logging.getLogger(__name__).debug(f"Could not determine the health of the session: {e}")


class SessionLifecycleManager(LifecycleManager):
    def __init__(self, session_id: str, aura_api: AuraApi):
        self.session_id = session_id
        self._aura_api = aura_api

    def delete(self) -> bool:
        return self._aura_api.delete_session(self.session_id)

    def verify_health(self) -> None:
        # Raises a SessionStatusError if the session is in an unhealthy state
        details = self._aura_api.get_session_with_errors(self.session_id)

        if details is None:
            raise SessionStatusError(
                [],
                hint=f"Session `{self.session_id}` does not exist any more."
                " It was either deleted or expired. Create a new session to continue.",
            )

        if details.is_ready():
            return

        if details.is_failed() or details.is_expired() or details.is_deleted():
            raise SessionStatusError(details.errors or [], details)

        raise SessionStatusError(
            details.errors or [],
            details,
            hint=f"The session is not ready to be used yet (status `{details.status}`).",
        )


class Noop(LifecycleManager):
    def delete(self) -> bool:
        return True

    def verify_health(self) -> None:
        pass
