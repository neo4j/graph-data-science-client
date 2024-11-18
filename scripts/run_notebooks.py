#!/usr/bin/env python3

import signal
import sys
from datetime import datetime
from pathlib import Path
from types import FrameType
from typing import Any, Callable, List, NamedTuple, Optional

import nbformat
from nbclient.exceptions import CellExecutionError
from nbconvert.preprocessors.execute import ExecutePreprocessor

VERSION_CELL_TAG = "verify-version"
TEARDOWN_CELL_TAG = "teardown"


class IndexedCell(NamedTuple):
    cell: Any
    index: int  # type: ignore


class GdsExecutePreprocessor(ExecutePreprocessor):
    def __init__(self, **kw: Any):
        super().__init__(**kw)  # type: ignore

    def init_notebook(self, version_cell_index: int, tear_down_cells: List[IndexedCell]) -> None:
        self.version_verify_cell_index = version_cell_index
        self.tear_down_cells = tear_down_cells
        self._skip_rest = False

    # run the cell of a notebook
    def preprocess_cell(self, cell: Any, resources: Any, index: int) -> None:
        if index == 0:

            def handle_signal(sig: int, frame: Optional[FrameType]) -> None:
                print("Received SIGNAL, running tear down cells")
                self.teardown(resources)
                sys.exit(1)

            signal.signal(signal.SIGINT, handle_signal)
            signal.signal(signal.SIGTERM, handle_signal)

        try:
            if not self._skip_rest:
                super().preprocess_cell(cell, resources, index)  # type: ignore
        except CellExecutionError as e:
            if e.ename == "AssertionError" and index == self.version_verify_cell_index:
                print("Skipping notebook due to incompatible GDS version")
                self._skip_rest = True
                return

            if self.tear_down_cells:
                print(f"Running tear down cells due to error in notebook execution: {e}")
                self.teardown(resources)
            raise e

    def teardown(self, resources: Any) -> None:
        for td_cell, td_idx in self.tear_down_cells:
            try:
                super().preprocess_cell(td_cell, resources, td_idx)  # type: ignore
            except CellExecutionError as td_e:
                print(f"Error running tear down cell {td_idx}: {td_e}")


class GdsTearDownCollector(ExecutePreprocessor):
    def __init__(self, **kw: Any):
        super().__init__(**kw)  # type: ignore

    def init_notebook(self) -> None:
        self._tear_down_cells: List[IndexedCell] = []

    def preprocess_cell(self, cell: Any, resources: Any, index: int) -> None:
        if TEARDOWN_CELL_TAG in cell["metadata"].get("tags", []):
            self._tear_down_cells.append(IndexedCell(cell, index))

    def tear_down_cells(self) -> List[IndexedCell]:
        return self._tear_down_cells


def main(filter_func: Callable[[str], bool]) -> None:
    examples_path = Path("examples")

    notebook_files = [
        f for f in examples_path.iterdir() if f.is_file() and f.suffix == ".ipynb" and filter_func(f.name)
    ]

    ep = GdsExecutePreprocessor(kernel_name="python3")
    td_collector = GdsTearDownCollector(kernel_name="python3")
    exceptions: List[RuntimeError] = []

    for notebook_filename in notebook_files:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{now}: Executing notebook {notebook_filename}", flush=True)

        with open(notebook_filename) as f:
            nb = nbformat.read(f, as_version=4)  # type: ignore

            # Collect tear down cells
            td_collector.init_notebook()
            td_collector.preprocess(nb)

            # Check if the GDS version matches
            # ep.execute_cell
            verify_version_cell_index = [
                idx for idx, cell in enumerate(nb["cells"]) if VERSION_CELL_TAG in cell["metadata"].get("tags", [])
            ]
            if not verify_version_cell_index or len(verify_version_cell_index) > 1:
                raise ValueError(
                    f"Notebook {notebook_filename} does not have a cell tagged with '{VERSION_CELL_TAG}'."
                    "Required to run the notebook only against compatible versions."
                )
            ep.init_notebook(
                version_cell_index=verify_version_cell_index[0], tear_down_cells=td_collector.tear_down_cells()
            )

            # run the notebook
            try:
                ep.preprocess(nb)
            except CellExecutionError as e:
                exceptions.append(RuntimeError(f"Error executing notebook {notebook_filename}", e))
                continue

    if exceptions:
        for nb_ex in exceptions:
            print(nb_ex)
        raise RuntimeError(f"{len(exceptions)} Errors occurred while executing notebooks")
    else:
        print(f"Finished executing notebook {notebook_filename}")


if __name__ == "__main__":
    notebook_filter = sys.argv[1] if len(sys.argv) >= 2 else ""

    session_notebooks = ["gds-sessions.ipynb"]
    session_self_managed_notebooks = ["gds-sessions-self-managed.ipynb"]

    notebooks: Optional[List[str]] = None
    if notebook_filter == "sessions-attached":

        def filter_func(notebook: str) -> bool:
            return notebook in session_notebooks
    elif notebook_filter == "sessions-self-managed-db":

        def filter_func(notebook: str) -> bool:
            return notebook in session_self_managed_notebooks
    else:

        def filter_func(notebook: str) -> bool:
            return notebook not in session_notebooks + session_self_managed_notebooks

    main(filter_func)
