from pathlib import Path
from typing import Any

import nbformat
from nbconvert.preprocessors import CellExecutionError, ExecutePreprocessor


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
ep = GdsExecutePreprocessor(timeout=600, kernel_name="python")

for notebook_filename in notebook_files:
    print(f"Executing notebook {notebook_filename}")
    with open(notebook_filename) as f:
        nb = nbformat.read(f, as_version=4)  # type: ignore

        # Check if the GDS version matches
        # ep.execute_cell
        version_verify_cell_index = [
            idx for idx, cell in enumerate(nb["cells"]) if "version-verify" in cell["metadata"].get("tags", [])
        ]
        if not version_verify_cell_index or len(version_verify_cell_index) > 1:
            raise ValueError(
                f"Notebook {notebook_filename} does not have a cell tagged with 'version-verify'."
                "Required to run the notebook only against compatible versions."
            )
        ep.init_notebook(version_verify_cell_index[0])
        # run the notebook
        ep.preprocess(nb)
    print(f"Finished executing notebook {notebook_filename}")
