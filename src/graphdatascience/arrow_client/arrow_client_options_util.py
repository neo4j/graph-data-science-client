from typing import Any

TLS_ROOT_CERTS_OPTION = "tls_root_certs"


def disable_server_verification(arrow_client_options: dict[str, Any]) -> dict[str, Any]:
    arrow_client_options["disable_server_verification"] = True
    return arrow_client_options


def set_tls_root_certs(arrow_client_options: dict[str, Any], tls_root_certs: bytes | str) -> dict[str, Any]:
    arrow_client_options[TLS_ROOT_CERTS_OPTION] = tls_root_certs
    return arrow_client_options
