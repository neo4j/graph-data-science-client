"""Unified environment-variable handling for the ``gds`` CLI.

``gds database`` and ``gds session`` connect to the *same* Aura database, so they
read the *same* env set. This module reads it once and hands back whichever
object each flow needs:

* ``gds database`` writes/reads directly with the ``neo4j`` driver -> :class:`DatabaseConfig`
* ``gds session`` drives a GDS session -> :class:`~graphdatascience.session.DbmsConnectionInfo`
  + :class:`~graphdatascience.session.AuraAPICredentials`

Unified env set:
    NEO4J_URI         bolt URI, e.g. neo4j+s://<instance>.databases.neo4j.io
    NEO4J_USERNAME    DB auth (both command groups)
    NEO4J_PASSWORD    DB auth (both command groups)
    NEO4J_DATABASE    target database (default "neo4j")
    AURA_INSTANCEID   Aura instance id (derived from NEO4J_URI host if unset)
    CLIENT_ID         Aura API credentials (gds session)
    CLIENT_SECRET     Aura API credentials (gds session)
    PROJECT_ID        Aura API project id (optional, gds session)
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from dotenv import dotenv_values

from graphdatascience.session import AuraAPICredentials, DbmsConnectionInfo

DEFAULT_DATABASE = "neo4j"


def load_env(env_file: str | Path | None = None) -> None:
    """Populate ``os.environ`` from dotenv files, never overriding existing vars.

    Precedence, highest first:

    1. variables already in the process environment (e.g. the k8s secret);
    2. an explicit ``env_file`` (when passed via ``--env-file``);
    3. a ``.env`` discovered by walking up from the current working directory.

    Because every load uses ``override=False``, real environment variables always
    win — dotenv files only fill in what is missing.
    """
    from dotenv import find_dotenv, load_dotenv

    if env_file is not None:
        load_dotenv(env_file, override=False)
    discovered = find_dotenv(usecwd=True)
    if discovered:
        load_dotenv(discovered, override=False)


@dataclass
class DatabaseConfig:
    """Connection details for the raw ``neo4j`` Python driver."""

    uri: str
    username: str
    password: str
    database: str = DEFAULT_DATABASE

    @property
    def auth(self) -> tuple[str, str]:
        return (self.username, self.password)


def _require(name: str, env: Mapping[str, str | None] = os.environ) -> str:
    value = env.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable {name!r} is not set")
    return value


def instance_id_from_uri(uri: str) -> str | None:
    """Extract the Aura instance id from a bolt URI.

    Aura URIs look like ``neo4j+s://<instance>.databases.neo4j.io``; the instance
    id is the first host label.
    """
    host = urlparse(uri).hostname
    if not host:
        return None
    return host.split(".", 1)[0] or None


def database_config_from_env(env: Mapping[str, str | None] = os.environ) -> DatabaseConfig:
    return DatabaseConfig(
        uri=_require("NEO4J_URI", env),
        username=_require("NEO4J_USERNAME", env),
        password=_require("NEO4J_PASSWORD", env),
        database=env.get("NEO4J_DATABASE") or DEFAULT_DATABASE,
    )


def database_config_from_dotenv(path: str | Path) -> DatabaseConfig:
    """Build a :class:`DatabaseConfig` from a dotenv file, without touching ``os.environ``."""
    return database_config_from_env(dotenv_values(Path(path).expanduser()))


def aura_instance_id_from_env() -> str:
    instance_id = os.environ.get("AURA_INSTANCEID")
    if instance_id:
        return instance_id
    uri = os.environ.get("NEO4J_URI")
    if uri:
        derived = instance_id_from_uri(uri)
        if derived:
            return derived
    raise RuntimeError("Set AURA_INSTANCEID or NEO4J_URI to determine the Aura instance id")


def dbms_connection_info_from_env() -> DbmsConnectionInfo:
    """Build a :class:`~graphdatascience.session.DbmsConnectionInfo` from the unified env set."""
    return DbmsConnectionInfo(
        username=_require("NEO4J_USERNAME"),
        password=_require("NEO4J_PASSWORD"),
        aura_instance_id=aura_instance_id_from_env(),
    )


def aura_api_credentials_from_env() -> AuraAPICredentials:
    """Build :class:`~graphdatascience.session.AuraAPICredentials` from the unified env set."""
    return AuraAPICredentials(
        client_id=_require("CLIENT_ID"),
        client_secret=_require("CLIENT_SECRET"),
        project_id=os.environ.get("PROJECT_ID"),
    )
