#!/bin/bash

DOC_DIR=doc/modules/ROOT/pages/tutorials
NB_DIR=examples

for notebook in ${NB_DIR}/*.ipynb
do
  docfile=$(basename ${notebook} | cut -d. -f1)
  echo "${notebook} -> ${DOC_DIR}/${docfile}.adoc"

  NB=$(cat $notebook)
  FORMATTED_NB=$(jupyter nbconvert \
    --clear-output \
    --stdout \
    --to asciidoc \
    --template=scripts/nb2doc/asciidoc-template \
    --ASCIIDocExporter.file_extension=.adoc \
    --no-prompt \
    --ClearMetadataPreprocessor.enabled=True \
    --log-level CRITICAL \
    $notebook)

  if [[ "$FORMATTED_NB" != "$NB" ]];
  then
    echo "Run convert.sh to update docs"
    exit 1
  fi
done

