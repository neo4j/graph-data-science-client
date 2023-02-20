#!/bin/bash

DOC_DIR=doc/modules/ROOT/pages/tutorials
NB_DIR=examples

for notebook in ${NB_DIR}/*.ipynb
do
  docfile=$(basename ${notebook} | cut -d. -f1)
  echo "${notebook} -> ${DOC_DIR}/${docfile}.adoc"

  # --noprompt
  #   Skips the "In/Out" lines before each cell
  # --ClearMetadataPreprocessor.enabled=True
  #   Cleans the "ipython3" language replacing it with "Python"
  #   (for Asciidoc code cells)
  # --ASCIIDocExporter.file_extension=.adoc
  #   If not set, the extension is .asciidoc

  jupyter nbconvert \
    --to asciidoc \
    --template=scripts/nb2doc/asciidoc-template \
    --output-dir ${DOC_DIR} \
    --ASCIIDocExporter.file_extension=.adoc \
    --no-prompt \
    --ClearMetadataPreprocessor.enabled=True \
    ${notebook}
done

