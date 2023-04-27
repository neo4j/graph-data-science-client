import os
import re
from typing import Any, List

# Check that all the functions annotated with @client_only_endpoints are covered in sphinx docs.

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


def find_single_client_only_functions(py_file_path: str):  # type: ignore
    with open(py_file_path) as f:
        contents = f.read()
        matches = re.finditer(CLIENT_ONLY_PATTERN, contents)
        return [(match.group(1), match.group(3)) for match in matches]


def find_client_only_functions() -> List[str]:
    client_only_functions = []
    for root, dirs, files in os.walk(PROJECT_DIR):
        for file in files:
            if file.endswith(".py"):
                filepath = os.path.join(root, file)
                client_only_functions += find_single_client_only_functions(filepath)
    return client_only_functions


def check_rst_files(client_only_functions) -> Any:  # type: ignore
    not_mentioned = set(client_only_functions)
    for root, dirs, files in os.walk(RST_DIR):
        for file in files:
            if file.endswith(".rst"):
                filepath = os.path.join(root, file)
                with open(filepath) as f:
                    content = f.read()
                    for namespace, function_name in client_only_functions:
                        fullname = f"{namespace}.{function_name}"
                        if fullname in content:
                            not_mentioned.discard((namespace, function_name))
    for namespace, function_name in not_mentioned:
        print(f"{namespace}.{function_name} is not mentioned in any RST files")

    if len(not_mentioned) > 0:
        raise Exception("Some of the client_only_endpoints are not in RST files. They don't show in the sphinx doc.")
    else:
        print("All client_only_endpoints are documented in RST files.")


if __name__ == "__main__":
    client_only_functions = find_client_only_functions()
    check_rst_files(client_only_functions)
