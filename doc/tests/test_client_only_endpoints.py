import os
import re
import sys
from typing import List

from neo4j import GraphDatabase

sys.path.append("../../graphdatascience")

from graphdatascience.ignored_server_endpoints import (  # noqa: E402
    IGNORED_SERVER_ENDPOINTS,
)

URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
AUTH = ("neo4j", "password")
if os.environ.get("NEO4J_USER"):
    AUTH = (
        os.environ.get("NEO4J_USER", "DUMMY"),
        os.environ.get("NEO4J_PASSWORD", "neo4j"),
    )

# Project directory where Python files are located
PROJECT_DIR = "graphdatascience/"

# RST files directory where the function names should be mentioned
RST_DIR = "doc/sphinx/source/"

# Regex pattern to match the @client_only_endpoint annotation
# Client_only_endpoint def potentially have other annotations too.
# This extracts the namespace and def name of any client_only_endpoint
CLIENT_ONLY_PATTERN = re.compile(r"@client_only_endpoint\(\"([\w\.]+)\"\)\s*\n*((?:(?:@[^\n]+\n)*?)\s*)def (\w+)\(")

# Regex pattern to match function definitions
FUNCTION_DEF_PATTERN = re.compile(r"def\s+(\w+)\s*\(")


def find_single_client_only_functions(py_file_path: str) -> List[str]:
    with open(py_file_path) as f:
        contents = f.read()
        matches = re.finditer(CLIENT_ONLY_PATTERN, contents)
        return [f"{match.group(1)}.{match.group(3)}" for match in matches]


def find_client_only_functions() -> List[str]:
    client_only_functions = []
    for root, dirs, files in os.walk(PROJECT_DIR):
        for file in files:
            if not file.endswith(".py"):
                continue
            filepath = os.path.join(root, file)
            client_only_functions += find_single_client_only_functions(filepath)
    return client_only_functions


def find_covered_server_endpoints() -> List[str]:
    driver = GraphDatabase.driver(URI, auth=AUTH)
    with driver.session() as session:
        all_server_endpoints = session.run("CALL gds.list() YIELD name", {}).data()

    driver.close()

    return [ep["name"] for ep in all_server_endpoints if ep["name"] not in IGNORED_SERVER_ENDPOINTS]


def check_rst_files(endpoints: List[str]) -> None:
    not_mentioned = set(endpoints)

    for root, _, files in os.walk(RST_DIR):
        for file in files:
            if not file.endswith(".rst"):
                continue
            filepath = os.path.join(root, file)
            with open(filepath) as f:
                content = f.read()
                for ep in endpoints:
                    if ep in content:
                        not_mentioned.discard(ep)

    assert len(not_mentioned) == 0, (
        f"The following endpoints are not covered by reference documentation:"
        f"{os.linesep}{f',{os.linesep}'.join(not_mentioned)}"
    )


def test_client_only_endpoint_coverage() -> None:
    client_only_functions = find_client_only_functions()
    covered_server_endpoints = find_covered_server_endpoints()
    check_rst_files(client_only_functions + covered_server_endpoints)
