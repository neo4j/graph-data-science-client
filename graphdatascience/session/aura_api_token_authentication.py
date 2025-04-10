from graphdatascience.query_runner.arrow_authentication import ArrowAuthentication
from graphdatascience.session.aura_api import AuraApi


class AuraApiTokenAuthentication(ArrowAuthentication):
    def __init__(self, aura_api: AuraApi):
        self._aura_api = aura_api

    def auth_pair(self) -> tuple[str, str]:
        return "", self._aura_api._auth._auth_token()
