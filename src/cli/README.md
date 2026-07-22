# gds-cli

The `gds` command-line interface for the [Neo4j Graph Data Science Python client](https://github.com/neo4j/graph-data-science-client).

> ⚠️ **Experimental, development-only tool.** `gds-cli` is a separate package
> (`gds-cli`) that lives in the same repository as `graphdatascience` but is
> intentionally *not* installed when you `pip install graphdatascience`. It is
> not part of the supported client API and may change or break without notice.

It provides two command groups:

- `gds database ...` — generate test graphs, upload them to a Neo4j database, and read them back.
- `gds session ...` — create/manage a managed Aura GDS session and run a standardized GDS job (project → algorithms → writeback) against it.

## Install

From a clone (editable, via the repo's `justfile`):

```bash
just install-cli
# == uv tool install --editable ./src/cli --force
```

From git (installs `graphdatascience` at the same commit via the uv workspace):

```bash
uv tool install "git+https://github.com/neo4j/graph-data-science-client.git#subdirectory=src/cli"
```

Then:

```bash
gds --help
```

## Configuration

Both command groups read the **same** environment set, so `gds database` and
`gds session` always target the same Aura database. A `.env` in the working
directory is loaded automatically; pass `--env-file` for another dotenv file.
Real environment variables always take precedence over dotenv files.

| Variable | Used by | Description |
| --- | --- | --- |
| `NEO4J_URI` | both | Bolt URI, e.g. `neo4j+s://<instance>.databases.neo4j.io` |
| `NEO4J_USERNAME` | both | Database auth |
| `NEO4J_PASSWORD` | both | Database auth |
| `NEO4J_DATABASE` | both | Target database (default `neo4j`) |
| `AURA_INSTANCEID` | session | Aura instance id (derived from `NEO4J_URI` host if unset) |
| `CLIENT_ID` | session | Aura API credentials |
| `CLIENT_SECRET` | session | Aura API credentials |
| `PROJECT_ID` | session | Aura API project id (optional) |

## Usage

### `gds database`

```bash
gds database upload  -f graph.json     # seed test data (JSON construct format or random-graph spec)
gds database summary                   # compact counts + property names of uploaded test data
gds database fetch   -o json > g.json  # dump test data (round-trips back into `upload -f`)
gds database delete  --all             # remove uploaded test data (Dev-labelled)
```

Uploaded nodes get an extra `Dev` label so test data can be found and cleaned up
without touching real data. `upload` asks before replacing existing same-labelled
data unless you pass `--overwrite`.

### `gds session`

```bash
gds session create                # create (or reconnect to) the session in the config
gds session run    -f job.yaml    # whole job: project → algorithms → writeback, per graph
gds session project|algorithms|writeback|drop  # run one step at a time
gds session list                  # list sessions visible to the configured credentials
gds session delete                # delete the session in the config
```

The job config is a JSON-schema-validated YAML document passed with `--file`/`-f`,
or, if omitted, read from the `$GDS_JOB_CONFIG` environment variable (handy for
carrying config inline in a single k8s Job resource).

Run `gds --help`, `gds database --help`, or `gds session --help` for full details.
