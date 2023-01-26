#!/bin/bash

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
  --output-dir doc/modules/ROOT/pages/tutorials/ \
  --output load-data-via-graph-construction-generated \
  --ASCIIDocExporter.file_extension=.adoc \
  --no-prompt \
  --ClearMetadataPreprocessor.enabled=True \
  examples/load-data-via-graph-construction.ipynb

