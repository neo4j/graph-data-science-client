from __future__ import annotations

from typing import Any

from pyarrow._flight import ClientMiddleware, ClientMiddlewareFactory


class UserAgentFactory(ClientMiddlewareFactory):  # type: ignore
    def __init__(self, useragent: str, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._middleware = UserAgentMiddleware(useragent)

    def start_call(self, info: Any) -> ClientMiddleware:
        return self._middleware


class UserAgentMiddleware(ClientMiddleware):  # type: ignore
    def __init__(self, useragent: str, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._useragent = useragent

    def sending_headers(self) -> dict[str, str]:
        return {"x-gds-user-agent": self._useragent}

    def received_headers(self, headers: dict[str, Any]) -> None:
        pass
