from __future__ import annotations

import base64
import time
from typing import Any, Optional

from pyarrow._flight import ClientMiddleware, ClientMiddlewareFactory

from graphdatascience.arrow_client.arrow_authentication import ArrowAuthentication


class AuthFactory(ClientMiddlewareFactory):  # type: ignore
    def __init__(self, middleware: AuthMiddleware, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._middleware = middleware

    def start_call(self, info: Any) -> AuthMiddleware:
        return self._middleware


class AuthMiddleware(ClientMiddleware):  # type: ignore
    def __init__(self, auth: ArrowAuthentication, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._auth = auth
        self._token: Optional[str] = None
        self._token_timestamp = 0

    def token(self) -> Optional[str]:
        # check whether the token is older than 10 minutes. If so, reset it.
        if self._token and int(time.time()) - self._token_timestamp > 600:
            self._token = None

        return self._token

    def _set_token(self, token: str) -> None:
        self._token = token
        self._token_timestamp = int(time.time())

    def received_headers(self, headers: dict[str, Any]) -> None:
        auth_header = headers.get("authorization", None)
        if not auth_header:
            return

        # the result is always a list
        header_value = auth_header[0]

        if not isinstance(header_value, str):
            raise ValueError(f"Incompatible header value received from server: `{header_value}`")

        auth_type, token = header_value.split(" ", 1)
        if auth_type == "Bearer":
            self._set_token(token)

    def sending_headers(self) -> dict[str, str]:
        token = self.token()
        if token is not None:
            return {"authorization": "Bearer " + token}

        auth_pair = self._auth.auth_pair()
        auth_token = f"{auth_pair[0]}:{auth_pair[1]}"
        auth_token = "Basic " + base64.b64encode(auth_token.encode("utf-8")).decode("ASCII")
        # There seems to be a bug, `authorization` must be lower key
        return {"authorization": auth_token}
