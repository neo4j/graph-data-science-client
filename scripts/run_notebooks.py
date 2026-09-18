#!/usr/bin/env python3

import logging
import os
import re
import signal
import sys
from enum import Enum
from pathlib import Path
from types import FrameType
from typing import NamedTuple

import nbformat
from nbclient.exceptions import CellExecutionError
from nbconvert.preprocessors.execute import ExecutePreprocessor
from nbformat import NotebookNode

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Matches ANSI escape sequences (colors, cursor moves) that IPython embeds in tracebacks.
ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")

VERSION_CELL_TAG = "verify-version"
TEARDOWN_CELL_TAG = "teardown"

# A notebook is classified by how it creates its GDS client: constructing a
# `GdsSessions` instance (or calling `get_or_create`) makes it a session notebook,
# constructing a `GraphDataScience` client directly makes it a plugin notebook.
SESSION_CONSTRUCTOR_RE = re.compile(r"\bGdsSessions\s*\(")
GET_OR_CREATE_RE = re.compile(r"\.get_or_create\s*\(")
GDS_CONSTRUCTOR_RE = re.compile(r"\bGraphDataScience\s*\(")
AURA_ATTACHED_RE = re.compile(r"\baura_instance_id\s*=|\bAURA_INSTANCEID\b")


class NotebookKind(Enum):
    PLUGIN = "plugin"
    SESSION = "session"
    SESSION_AURA_ATTACHED = "session-aura-attached"


SESSION_KINDS = (NotebookKind.SESSION, NotebookKind.SESSION_AURA_ATTACHED)


def _code_source(nb: NotebookNode) -> str:
    return "\n".join(cell["source"] for cell in nb["cells"] if cell["cell_type"] == "code")


def classify_notebook(nb: NotebookNode, notebook_name: str) -> NotebookKind:
    """Classify a notebook by how it creates its GDS client.

    Session notebooks construct a `GdsSessions` instance or call `get_or_create`;
    plugin notebooks construct a `GraphDataScience` client directly. Notebooks doing
    neither are rejected so that they are not silently run in the wrong CI job.
    """
    code = _code_source(nb)

    if SESSION_CONSTRUCTOR_RE.search(code) or GET_OR_CREATE_RE.search(code):
        # a session marker wins: a plugin constructor in the same notebook might just be
        # a commented-out alternative
        if AURA_ATTACHED_RE.search(code):
            return NotebookKind.SESSION_AURA_ATTACHED
        return NotebookKind.SESSION

    if GDS_CONSTRUCTOR_RE.search(code):
        return NotebookKind.PLUGIN

    raise RuntimeError(
        f"Cannot classify notebook '{notebook_name}': found neither a `GdsSessions(...)`, "
        "`.get_or_create(...)`, nor `GraphDataScience(...)` client construction in its code cells."
    )


# The `resources` dict nbconvert threads through the preprocessor chain. We only pass it along.
Resources = dict[str, object]

# Matches `session_name="..."` (also the `session_name = '...'` assignment form) in a code cell.
SESSION_NAME_RE = re.compile(r"""(session_name\s*=\s*)(["'])(.+?)\2""")


def _session_name_suffix() -> str | None:
    """Unique per-build suffix so concurrent CI builds don't share a session name."""
    suffix = os.environ.get("NOTEBOOK_SESSION_SUFFIX")
    if not suffix:
        build_id = os.environ.get("BUILD_ID")
        suffix = f"ci{build_id}" if build_id else None
    return re.sub(r"[^A-Za-z0-9-]", "-", suffix) if suffix else None


def _apply_session_name_suffix(nb: NotebookNode, suffix: str, notebook_name: str) -> None:
    """Rename the sessions of an in-memory notebook. The notebook file itself is never written back."""

    renamed: dict[str, str] = {}

    def rename(match: re.Match[str]) -> str:
        assignment, quote, name = match.group(1), match.group(2), match.group(3)
        # keep the total length bounded in case the build id is long
        new_name = f"{name}-{suffix}"
        renamed[name] = new_name
        return f"{assignment}{quote}{new_name}{quote}"

    cells: list[NotebookNode] = nb["cells"]
    for cell in cells:
        if cell["cell_type"] == "code":
            cell["source"] = SESSION_NAME_RE.sub(rename, cell["source"])

    # a session name usually shows up twice: once when creating and once in the teardown cell
    for name, new_name in renamed.items():
        logger.info(
            "[%s] renaming session %s -> %s to avoid clashes between concurrent runs", notebook_name, name, new_name
        )


class IndexedCell(NamedTuple):
    cell: NotebookNode
    index: int  # type: ignore


def _indent(text: str, prefix: str = "    | ") -> str:
    return "\n".join(prefix + line for line in text.splitlines())


class GdsExecutePreprocessor(ExecutePreprocessor):
    def __init__(self, **kw: object):
        super().__init__(**kw)  # type: ignore

    def init_notebook(
        self,
        notebook_name: str,
        total_code_cells: int,
        version_cell_index: int | None,
        tear_down_cells: list[IndexedCell],
    ) -> None:
        self.notebook_name = notebook_name
        self.total_code_cells = total_code_cells
        self.version_verify_cell_index = version_cell_index
        self.tear_down_cells = tear_down_cells
        self._skip_rest = False
        self._code_cell_count = 0
        self.failed_cell_number: int | None = None
        self.failed_cell_source: str = ""

    # run the cell of a notebook
    def preprocess_cell(self, cell: NotebookNode, resources: Resources, index: int) -> None:
        if index == 0:

            def handle_signal(sig: int, frame: FrameType | None) -> None:
                logger.warning("Received SIGNAL, running tear down cells")
                self.teardown(resources)
                sys.exit(1)

            signal.signal(signal.SIGINT, handle_signal)
            signal.signal(signal.SIGTERM, handle_signal)

        if cell.cell_type == "code" and not self._skip_rest:
            self._code_cell_count += 1
            logger.info("[%s] executing cell %d/%d", self.notebook_name, self._code_cell_count, self.total_code_cells)

        try:
            if not self._skip_rest:
                super().preprocess_cell(cell, resources, index)  # type: ignore
        except CellExecutionError as e:
            if (
                self.version_verify_cell_index
                and e.ename == "AssertionError"
                and index == self.version_verify_cell_index
            ):
                logger.info("Skipping notebook %s due to incompatible GDS version", self.notebook_name)
                self._skip_rest = True
                return

            # Remember the failing cell so the caller can point the user straight at it.
            self.failed_cell_number = self._code_cell_count
            self.failed_cell_source = cell.source

            if self.tear_down_cells:
                logger.error("Running tear down cells due to error in notebook execution: %s", e)
                self.teardown(resources)
            raise e

    def teardown(self, resources: Resources) -> None:
        for td_cell, td_idx in self.tear_down_cells:
            try:
                super().preprocess_cell(td_cell, resources, td_idx)  # type: ignore
            except CellExecutionError as td_e:
                logger.error("Error running tear down cell %d: %s", td_idx, td_e)


class GdsTearDownCollector(ExecutePreprocessor):
    def __init__(self, **kw: object):
        super().__init__(**kw)  # type: ignore

    def init_notebook(self) -> None:
        self._tear_down_cells: list[IndexedCell] = []

    def preprocess_cell(self, cell: NotebookNode, resources: Resources, index: int) -> None:
        if TEARDOWN_CELL_TAG in cell["metadata"].get("tags", []):
            self._tear_down_cells.append(IndexedCell(cell, index))

    def tear_down_cells(self) -> list[IndexedCell]:
        return self._tear_down_cells


class LoadedNotebook(NamedTuple):
    path: Path
    nb: NotebookNode
    kind: NotebookKind


def load_notebooks() -> list[LoadedNotebook]:
    examples_path = Path("examples")

    loaded: list[LoadedNotebook] = []
    for path in sorted(examples_path.iterdir()):
        if not (path.is_file() and path.suffix == ".ipynb"):
            continue

        with open(path) as f:
            nb = nbformat.read(f, as_version=4)  # type: ignore

        kind = classify_notebook(nb, path.name)
        logger.info("Classified %s as %s", path.name, kind.value)
        loaded.append(LoadedNotebook(path, nb, kind))

    return loaded


def main(notebooks: list[LoadedNotebook]) -> None:
    ep = GdsExecutePreprocessor(kernel_name="python3")
    td_collector = GdsTearDownCollector(kernel_name="python3")
    failures: list[tuple[str, CellExecutionError, int | None]] = []

    logger.info("Found notebooks to execute: %s", [n.path.name for n in notebooks])

    session_name_suffix = _session_name_suffix()

    for notebook in notebooks:
        logger.info("Executing notebook %s", notebook.path)

        nb = notebook.nb

        if session_name_suffix and notebook.kind in SESSION_KINDS:
            _apply_session_name_suffix(nb, session_name_suffix, notebook.path.name)

        # Collect tear down cells
        td_collector.init_notebook()
        td_collector.preprocess(nb)

        # Check if the GDS version matches
        # ep.execute_cell
        verify_version_cell_index = [
            idx for idx, cell in enumerate(nb["cells"]) if VERSION_CELL_TAG in cell["metadata"].get("tags", [])
        ]

        ep.init_notebook(
            notebook_name=notebook.path.name,
            total_code_cells=sum(1 for cell in nb["cells"] if cell["cell_type"] == "code"),
            version_cell_index=verify_version_cell_index[0] if verify_version_cell_index else None,
            tear_down_cells=td_collector.tear_down_cells(),
        )

        # run the notebook
        try:
            ep.preprocess(nb)
        except CellExecutionError as e:
            failures.append((notebook.path.name, e, ep.failed_cell_number))
            # Concise summary pointing at the failing cell; full (ANSI-stripped) traceback at DEBUG.
            logger.error(
                "Failed notebook %s at code cell %s -- %s: %s",
                notebook.path.name,
                ep.failed_cell_number,
                e.ename,
                e.evalue,
            )
            logger.error("Failing cell content:\n%s", _indent(ep.failed_cell_source))
            logger.debug("Traceback for %s:\n%s", notebook.path.name, ANSI_ESCAPE.sub("", str(e)))
            continue

    if failures:
        logger.error("%d of %d notebooks failed:", len(failures), len(notebooks))
        for nb_name, err, cell_number in failures:
            logger.error("  - %s (code cell %s): %s: %s", nb_name, cell_number, err.ename, err.evalue)
        raise SystemExit(1)
    else:
        logger.info("Successfully executed %d notebook(s)", len(notebooks))


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv[1:]
    positional = [arg for arg in sys.argv[1:] if not arg.startswith("--")]
    notebook_filter = positional[0] if positional else ""

    logger.info("Notebook filter: %s", notebook_filter)

    all_notebooks = load_notebooks()

    if notebook_filter == "sessions-attached":
        selected = [n for n in all_notebooks if n.kind is NotebookKind.SESSION_AURA_ATTACHED]
    elif notebook_filter == "sessions-self-managed-db":
        selected = [n for n in all_notebooks if n.kind is NotebookKind.SESSION]
    elif notebook_filter:
        selected = [n for n in all_notebooks if notebook_filter in n.path.name]
    else:
        selected = [n for n in all_notebooks if n.kind is NotebookKind.PLUGIN]

    if dry_run:
        selected_paths = {n.path for n in selected}
        for n in all_notebooks:
            state = "selected" if n.path in selected_paths else "skipped"
            logger.info("[dry-run] %-55s %-23s %s", n.path.name, n.kind.value, state)
        logger.info("[dry-run] %d of %d notebook(s) selected", len(selected), len(all_notebooks))
        raise SystemExit(0)

    main(selected)
