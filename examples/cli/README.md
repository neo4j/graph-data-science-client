# `gds` CLI examples

Sample graphs and ready-to-run job configs for the `gds` command-line tool (see
the [Command-line interface](../../README.md#command-line-interface-gds)
section of the main README, and [architecture.md](../../src/cli/architecture.md)
for how a job config is executed).

Prereq: `pip install "graphdatascience[cli]"` (or `just install-cli` from the repo
root), and Aura credentials. Copy `env.template` to `.env` in this directory and
fill it in — `gds` auto-loads a `.env` from the current working directory, so run
the commands below from **this `examples/cli/` directory**.

```bash
cp env.template .env
# fill in .env, then run commands from this directory
```

## What's here

Three graphs; the **social graph is reused across many algorithms**.

**Shared social graph** (`graphs/social-network.yaml`) — a generated
`Person -KNOWS-> Person` graph (`kind: random`, power-law) carrying an `age` node
property (a FastRP feature) and a positive `weight` relationship property (for
weighted PageRank), so every social job runs against the same upload.

Upload it once, then run any of these jobs against it:

| Job | Algorithm(s) | Writes |
| --- | --- | --- |
| `jobs/wcc.yaml` | Weakly Connected Components (simplest: one native projection, one algorithm) | `componentId` |
| `jobs/social-network.yaml` | weighted PageRank → FastRP → Louvain, **one projection reused** | `pagerank`, `embedding`, `community` |

**Citation graph** (`graphs/citation.yaml`) — a generated `Paper -CITES-> Paper`
graph, used alongside the social graph by `jobs/multi-graph.yaml` (one job per graph).

**Temporal graph (FastPath)** — `Entity -HAS_EVENT-> Event`, a distinct shape
needed by the FastPath algorithm:

- `graphs/entity-event.json` with `jobs/fastpath.yaml` → `fp_embeddings`.

Fixed graphs are `.json` (construct format); generated graphs are `.yaml`
(`kind: random` specs). By default each `gds run` spins up a fresh, uniquely-named
session and deletes it when the run finishes; pass `--session-name <name>` to
reuse (and keep) one warm session across runs. `--overwrite-graph` replaces the
projected graph each time.

### Reuse across algorithms

- **Upload once, run many:** every social job projects the same `Person/KNOWS`
  data, so a single `gds database upload` feeds PageRank, Louvain, WCC, FastRP, …
- **One projection, many algorithms in a run:** `jobs/social-network.yaml` runs
  three algorithms on a *single* projection — PageRank is mutated into the graph so
  FastRP can consume it as a feature, then Louvain runs on the same graph.
- **Many jobs in a run:** `jobs/multi-graph.yaml` has two jobs projecting
  *different graphs* — Louvain on the social graph, PageRank on the citation graph
  — each an isolated project→compute→write→drop unit.

### Projection kinds

These examples use **native** projections (`nodeLabels` + `relationshipTypes`, read
straight from the DB, with optional `nodeProperties` / `relationshipProperties`).
`jobs/fastpath.yaml` is the exception — it uses a **cypher** projection (a `query`
returning `gds.graph.project.remote(...)`) to forward the temporal node properties
FastPath needs.

## Run a job

Upload the shared graph once, then run any social job and inspect:

```bash
gds database upload -f graphs/social-network.yaml --overwrite
gds run -f jobs/wcc.yaml --overwrite-graph
gds database fetch --label Person        # componentId column populated

# reuse the SAME uploaded graph for more algorithms — no re-upload
gds run -f jobs/social-network.yaml --overwrite-graph
gds database fetch --label Person        # pagerank / community / embedding populated too
```

## Multiple graphs in one run

`jobs/multi-graph.yaml` runs a job per graph. Upload both first — they use disjoint
labels, so they coexist in the DB. `upload -f` is repeatable (and also accepts a
directory, e.g. `-f graphs/` for every graph in it):

```bash
gds database upload -f graphs/social-network.yaml -f graphs/citation.yaml --overwrite
gds run -f jobs/multi-graph.yaml --overwrite-graph
gds database fetch --label Person        # community
gds database fetch --label Paper         # pagerank
```

`gds run -f` also accepts a **directory** of configs: `gds run -f jobs/ --overwrite-graph`
runs every `jobs/*.yaml` (sorted), each handled independently — like invoking
`gds run` once per file, each in its own session. Upload all the graphs they need
first (`gds database upload -f graphs/`).

Each `gds run` (and each config in a directory run) uses a throwaway session by
default, created then deleted. To keep one warm session and reuse it across runs,
pass `--session-name <name>` (it is reused and left running until its TTL).

## FastPath (temporal) graph

```bash
gds database upload -f graphs/entity-event.json --overwrite
gds run -f jobs/fastpath.yaml --overwrite-graph
gds database fetch --label Entity
```

## Standalone sessions (no database)

A **standalone** session isn't attached to any database — it's created against a
cloud location (set `cloud` + `region` in the `session:` block), the graph is built
directly from a file (`project.type: construct`), and since there's no DB to write
back to, each `write` property is **streamed to an `outputFile`** (a sibling of
`nodeProperty`), resolved relative to the job config. Properties sharing the same
`outputFile` are written together (one file with all their columns); the format is
chosen by extension (`.csv`, or `.json` mirroring the construct input file with the
rows under a `computedNodeProperties` section). Only the Aura API credentials are needed
(`CLIENT_ID`/`CLIENT_SECRET`/`PROJECT_ID`) — no `NEO4J_*`.

Standalone examples live in their own folder, [`standalone/`](standalone/), where
the graph and output files sit next to the job (`construct` `file:` and each
`outputFile:` are resolved relative to the job config).
[`standalone/job.yaml`](standalone/job.yaml) runs PageRank and Louvain on
[`standalone/graph.json`](standalone/graph.json), both into one file:

```bash
gds run -f standalone/job.yaml
# -> standalone/result.json    (nodeId, pagerank, community)
```

No `gds database upload` step — the graph is constructed straight into the session.
(GDS `construct` ignores scalar string properties, so numeric-only node properties
load cleanly.)

## Cleanup

Clean up the database: `gds database delete --all`. Sessions clean up themselves —
each `gds run` deletes its throwaway session on completion. A session kept by name
(`--session-name` or `session.name`) lives until its `ttl` expires; delete it early
with `gds sessions delete --name <name>`.
