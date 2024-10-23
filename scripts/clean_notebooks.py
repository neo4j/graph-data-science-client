# reasons for not using nbconvert cli tool:
# * cannot keep output based on a given tag

import argparse
import logging
from enum import Enum
from pathlib import Path

import nbconvert
from nbconvert.preprocessors import Preprocessor

PRESERVE_CELL_OUTPUT_KEY = "preserve-output"
METADATA_TAG_KEY = "tags"


class OutputMode(Enum):
    STDOUT = "stdout"
    INPLACE = "inplace"


class CustomClearOutputPreprocessor(Preprocessor):
    """
    Removes the output from all code cells in a notebook.
    Option to keep cell output for cells with a given metadata tag
    """

    def preprocess_cell(self, cell, resources, cell_index):
        """
        Apply a transformation on each cell. See base.py for details.
        """
        if cell.cell_type == "code" and PRESERVE_CELL_OUTPUT_KEY not in cell["metadata"].get(METADATA_TAG_KEY, []):
            cell.outputs = []
            cell.execution_count = None
        return cell, resources


def main(input_path: Path, output_mode: str) -> None:
    logger = logging.getLogger("NotebookCleaner")
    logger.info(f"Cleaning notebooks from `{input_path}`, mode: `{output_mode}`")

    exporter = nbconvert.NotebookExporter()

    metadata_cleaner = nbconvert.preprocessors.ClearMetadataPreprocessor(preserve_cell_metadata_mask=METADATA_TAG_KEY)
    output_cleaner = CustomClearOutputPreprocessor()

    exporter.register_preprocessor(metadata_cleaner, enabled=True)
    exporter.register_preprocessor(output_cleaner, enabled=True)

    if input_path.is_file():
        notebooks = [input_path]
    else:
        notebooks = [f for f in input_path.iterdir() if f.is_file() and f.suffix == ".ipynb"]

    logger.info(f"Formatting {len(notebooks)} notebooks.")

    for notebook in notebooks:
        output = exporter.from_filename(notebook)

        formatted_notebook = output[0]

        if output_mode == OutputMode.INPLACE:
            with notebook.open(mode="w") as file:
                file.write(formatted_notebook)
        elif output_mode == OutputMode.STDOUT:
            print(formatted_notebook)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", choices=[e.value for e in OutputMode])
    parser.add_argument("-i", "--input", default="examples", help="path to the notebook file or folder")

    args = parser.parse_args()

    main(Path(args.input), OutputMode(args.output))
