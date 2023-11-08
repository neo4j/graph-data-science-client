from datetime import datetime
from pathlib import Path
from typing import Any

import nbformat
from nbclient.exceptions import CellExecutionError
from nbconvert.preprocessors.execute import ExecutePreprocessor

VERSION_CELL_TAG = "verify-version"


class GdsExecutePreprocessor(ExecutePreprocessor):
    def __init__(self, **kw: Any):
        super().__init__(**kw)  # type: ignore

    def init_notebook(self, index: int) -> None:
        self.version_verify_cell_index = index
        self._skip_rest = False

    def preprocess_cell(self, cell: Any, resources: Any, index: int) -> None:
        try:
            if not self._skip_rest:
                super().preprocess_cell(cell, resources, index)  # type: ignore
        except CellExecutionError as e:
            if e.ename == "AssertionError" and index == self.version_verify_cell_index:
                print("Skipping notebook due to incompatible GDS version")
                self._skip_rest = True
            else:
                raise e


examples_path = Path("examples")
notebook_files = [f for f in examples_path.iterdir() if f.is_file()]
ep = GdsExecutePreprocessor(kernel_name="python3")

for notebook_filename in notebook_files:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now}: Executing notebook {notebook_filename}", flush=True)
    with open(notebook_filename) as f:
        nb = nbformat.read(f, as_version=4)  # type: ignore

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
        ep.init_notebook(verify_version_cell_index[0])
        # run the notebook
        ep.preprocess(nb)
    print(f"Finished executing notebook {notebook_filename}")
