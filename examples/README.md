# Examples

This folder contains example notebooks on how to use the `graphdatascience` python client.

> There is also `gds`, an experimental, development-only command-line tool for
> seeding test data and running GDS session jobs. See [`../src/cli/README.md`](../src/cli/README.md).


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
