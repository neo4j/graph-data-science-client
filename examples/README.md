# Examples

This folder contains example notebooks on how to use the `graphdatascience` python client.

## Running/updating the notebooks locally

1. Copy `examples/sessions.env.template` → `examples/.env` and fill in your Aura credentials.
2. Use the project `.venv` by selecting it as the interpreter/kernel (run `uv sync --all-extras` first if you haven't).
3. Skip the `%pip install` cell at the top of each notebook - it is intended for fresh environments.


## Custom cell tags for notebooks

*Preserve cell outputs*

By default, `makestyle` will remove all cell outputs. If you want to preserve some outputs, tag the cell with `preserve-output`.


## Update /tutorials in docs

Every notebook is also available as `adoc` version living under `doc/pages/tutorials/`.
The latest published version can be viewed at https://neo4j.com/docs/graph-data-science-client/current/.

To update the adoc version, run

```bash
./scripts/nb2doc/convert.sh
```

On how to render the docs locally, the doc [README](../doc/README)
