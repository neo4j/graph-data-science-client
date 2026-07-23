# gds-cli

The `gds` command-line interface for the [Neo4j Graph Data Science Python client](https://github.com/neo4j/graph-data-science-client).

> ⚠️ **Experimental, development-only tool.** `gds-cli` is a separate package
> (`gds-cli`) that lives in the same repository as `graphdatascience` but is
> intentionally *not* installed when you `pip install graphdatascience`. It is
> not part of the supported client API and may change or break without notice.

It provides:

- `gds run ...` — run a standardized GDS job config (a `session` block + a list of `jobs`, each doing project → compute → write) against a managed Aura GDS session (created for the run and deleted afterwards, unless kept with `--session-name` or a named `session`).
- `gds database ...` (alias `gds db ...`) — generate test graphs, upload them to a Neo4j database, and read them back.
- `gds sessions ...` — create, list, and delete managed Aura GDS sessions directly (details from a config file and/or CLI flags).

See [architecture.md](architecture.md) for how a job config is interpreted and executed.

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

All commands read the **same** environment set, so `gds database`, `gds run`, and
`gds sessions` stay consistent about which Aura database and project they target. A
`.env` in the working directory is loaded automatically; pass `--env-file` for
another dotenv file. Real environment variables always take precedence over dotenv
files.

| Variable | Used by | Description |
| --- | --- | --- |
| `NEO4J_URI` | database, attached session | Bolt URI, e.g. `neo4j+s://<instance>.databases.neo4j.io` |
| `NEO4J_USERNAME` | database, attached session | Database auth |
| `NEO4J_PASSWORD` | database, attached session | Database auth |
| `NEO4J_DATABASE` | database, attached session | Target database (default `neo4j`) |
| `AURA_INSTANCEID` | session | Aura instance id (derived from `NEO4J_URI` host if unset) |
| `CLIENT_ID` | session | Aura API credentials |
| `CLIENT_SECRET` | session | Aura API credentials |
| `PROJECT_ID` | session | Aura API project id |

A **standalone** session (`session.cloud` + `session.region`, no attached database)
needs only the Aura API credentials (`CLIENT_ID`/`CLIENT_SECRET`/`PROJECT_ID`) — no
`NEO4J_*`.

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
data unless you pass `--overwrite`. `delete` reports how much it will remove and
asks for confirmation before deleting (pass `--yes`/`-y` to skip the prompt, or
`--dry-run` to only report).

### `gds run`

```bash
gds run -f job.yaml                       # whole config: each job projected → computed → written → dropped, in turn
gds run -f a.yaml -f b.yaml               # repeat -f for several configs; a folder works too (each run independently)
gds run -f jobs/                          # a directory: run every *.yaml config in it, each independently
gds run -f job.yaml --overwrite-graph     # drop an already-projected graph before re-projecting
gds run -f job.yaml --session-name warm   # reuse/create a session named "warm" and KEEP it after the run
```

By default `gds run` creates a fresh, uniquely-named session (`cli-<uuid4>`) and
deletes it once the run completes. To keep a warm session across runs, name it —
via `--session-name <name>` (or `GDS_RUNNER_SESSION_NAME`), or an optional
`session.name` in the config; `--session-name` wins. A named session is kept.

`--file`/`-f` is **repeatable** and each value may be a file or a **directory** (a
directory contributes its sorted `*.yaml`/`*.yml` configs). Every resolved config is
handled **independently** — exactly as if `gds run -f <file>` were invoked once per
file: each creates its own session from its own `session:` block (so they may differ
in memory / standalone-vs-attached) and, unless a name keeps it, deletes it before
the next config starts.

The job config is a JSON-schema-validated YAML document passed with `--file`/`-f`,
or, if omitted, read from the `$GDS_JOB_CONFIG` environment variable (handy for
carrying config inline in a single k8s Job resource).

### `gds sessions`

Manage sessions directly, independent of a run. Session details come from a config
file (`-f`) and/or CLI flags (`--name`/`--memory`/`--ttl`/`--cloud`/`--region`); CLI
flags override the config.

```bash
gds sessions create --name warm --memory 4GB --ttl 1h   # create from flags (ttl: 30m/2h/1d or minutes)...
gds sessions create -f job.yaml                          # ...or from a config's session block
gds sessions list                                        # list sessions for the current credentials
gds sessions delete --name warm                          # delete by name (asks first; -y to skip)
```

Run `gds --help`, `gds run --help`, `gds database --help`, or `gds sessions --help`
for full details.
