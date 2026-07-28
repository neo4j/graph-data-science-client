from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

INSTALLATION_ADOC_PATH = Path("doc") / "modules" / "ROOT" / "pages" / "installation.adoc"

CLIENT_COLUMN = "GDS Python Client"

PYTHON_ENV_COLUMN = "Python environment"
GDS_VERSION_COLUMN = "GDS version"
DRIVER_VERSION_COLUMN = "Neo4j Python Driver version"

# All compatibility tables, in the order they appear in installation.adoc
COMPATIBILITY_COLUMNS = (PYTHON_ENV_COLUMN, GDS_VERSION_COLUMN, DRIVER_VERSION_COLUMN)

TABLE_DELIMITER = "|==="


@dataclass
class CompatibilityTable:
    """A two-column compatibility table in installation.adoc."""

    column: str
    body: str
    # Span of `body` within the document it was parsed from
    start: int
    end: int
    # Table rows, each a list of cell lines. Rows are separated by blank lines.
    rows: list[list[str]]

    def latest_row(self) -> list[str]:
        if not self.rows:
            raise ValueError(f"The '{self.column}' table in installation.adoc has no entries")
        return self.rows[0]

    def latest_client_version(self) -> str:
        return cell_value(self.latest_row()[0])

    def latest_compatibility(self) -> str:
        row = self.latest_row()
        if len(row) < 2:
            found = "\n".join(row)
            raise ValueError(
                f"The latest entry of the '{self.column}' table in installation.adoc has no compatibility cell.\n"
                f"Found entry:\n{found}"
            )
        return cell_value(row[1])


def read_installation_doc(repo_dir: Path) -> str:
    return (repo_dir / INSTALLATION_ADOC_PATH).read_text()


def write_installation_doc(repo_dir: Path, content: str) -> None:
    (repo_dir / INSTALLATION_ADOC_PATH).write_text(content)


def cell_value(line: str) -> str:
    return line.lstrip("|").strip()


def find_table(content: str, column: str) -> CompatibilityTable:
    header = f"| {CLIENT_COLUMN} | {column}"
    header_match = re.search(rf"^{re.escape(header)}[^\n]*\n", content, re.MULTILINE)
    if not header_match:
        raise ValueError(f"Could not find the '{column}' table (header '{header}') in installation.adoc")

    start = header_match.end()
    end = content.find(f"\n{TABLE_DELIMITER}", start)
    if end == -1:
        raise ValueError(f"The '{column}' table in installation.adoc is not terminated by '{TABLE_DELIMITER}'")
    # Keep the trailing newline of the last entry as part of the body
    end += 1

    body = content[start:end]
    rows = [
        [line for line in block.splitlines() if line.strip()] for block in re.split(r"\n\s*\n", body) if block.strip()
    ]

    return CompatibilityTable(column=column, body=body, start=start, end=end, rows=rows)


def replace_table_body(content: str, table: CompatibilityTable, new_body: str) -> str:
    return content[: table.start] + new_body + content[table.end :]
