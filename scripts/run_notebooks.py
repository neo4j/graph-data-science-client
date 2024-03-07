#!/usr/bin/env python3

import sys
from datetime import datetime
from pathlib import Path
from typing import Any, List, NamedTuple

import nbformat
from nbclient.exceptions import CellExecutionError
from nbconvert.preprocessors.execute import ExecutePreprocessor

VERSION_CELL_TAG = "verify-version"
TEARDOWN_CELL_TAG = "teardown"


class IndexedCell(NamedTuple):
    cell: Any
    index: int


class GdsExecutePreprocessor(ExecutePreprocessor):
    def __init__(self, **kw: Any):
        super().__init__(**kw)  # type: ignore

    def init_notebook(self, version_cell_index: int, tear_down_cells: List[IndexedCell]) -> None:
        self.version_verify_cell_index = version_cell_index
        self.tear_down_cells = tear_down_cells
        self._skip_rest = False

    # run the cell of a notebook
    def preprocess_cell(self, cell: Any, resources: Any, index: int) -> None:
        try:
            if not self._skip_rest:
                super().preprocess_cell(cell, resources, index)  # type: ignore
        except CellExecutionError as e:
            if e.ename == "AssertionError" and index == self.version_verify_cell_index:
                print("Skipping notebook due to incompatible GDS version")
                self._skip_rest = True
            else:
                if self.tear_down_cells:
                    print("Running tear down cells")
                    for td_cell, td_idx in self.tear_down_cells:
                        super().preprocess_cell(td_cell, resources, td_idx)
                raise e


class GdsTearDownCollector(ExecutePreprocessor):
    def __init__(self, **kw: Any):
        super().__init__(**kw)  # type: ignore

    def init_notebook(self) -> None:
        self._tear_down_cells = []

    def preprocess_cell(self, cell: Any, resources: Any, index: int) -> None:
        if TEARDOWN_CELL_TAG in cell["metadata"].get("tags", []):
            self._tear_down_cells.append(IndexedCell(cell, index))

    def tear_down_cells(self) -> List[int]:
        return self._tear_down_cells


def main(run_session_nbs: bool) -> None:
    examples_path = Path("examples")

    notebook_files = [f for f in examples_path.iterdir() if f.is_file() and ("session" in f.name) == run_session_nbs]

    ep = GdsExecutePreprocessor(kernel_name="python3")
    td_collector = GdsTearDownCollector(kernel_name="python3")

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
            ep.init_notebook(verify_version_cell_index[0], td_collector.tear_down_cells())

            # run the notebook
            try:
                ep.preprocess(nb)
            except CellExecutionError as e:
                print(f"Error executing notebook {notebook_filename}: {e}")
                continue

        print(f"Finished executing notebook {notebook_filename}")


if __name__ == "__main__":
    notebook_filter = sys.argv[1] if len(sys.argv) >= 2 else ""
    only_session_nbs = True if notebook_filter == "sessions" else False

    main(only_session_nbs)
