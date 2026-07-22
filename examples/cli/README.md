# `gds` CLI examples

Sample graphs and ready-to-run job configs for the `gds` command-line tool (see
the [Command-line interface](../../README.md#command-line-interface-gds)
section of the main README). A small selection ported from the
[neo4j-cloud-job-scheduler-prototype](https://github.com/neo4j/neo4j-cloud-job-scheduler-prototype)'s
`gds-testdata`/`gds-runner` examples.

Prereq: `pip install "graphdatascience[cli]"` (or `just install-cli` from the repo
root), and Aura credentials. Copy `env.template` to `.env` in this directory and
fill it in — `gds` auto-loads a `.env` from the current working directory, so run
the commands below from **this `examples/cli/` directory**.

```bash
cp env.template .env
# fill in .env, then run commands from this directory
```

## What's here

Graphs (`graphs/`) and jobs (`jobs/`) share theme names, so they read as a story —
pick a graph, run the matching job:

Every graph under `graphs/` has a matching job under `jobs/`:

| Theme | Graph — fixed / random | Job |
| --- | --- | --- |
| social network | `graphs/social-network.json` | `jobs/social-network-louvain.yaml` → `community`; `jobs/social-network-native.yaml` (native) |
| random social | `graphs/random-social-network.yaml` | `jobs/random-social-network.yaml` → `pagerank` |
| people | `graphs/people.yaml` | `jobs/people.yaml` → `community` |
| web graph | `graphs/web-graph.json` | `jobs/web-graph-pagerank.yaml` → `pagerank`, `fastRP` |
| citation | `graphs/citation.yaml` | `jobs/citation.yaml` → `pagerank`, `fastRP` |
| homogeneous | `graphs/homogeneous.yaml` | `jobs/homogeneous.yaml` → `pagerank` |
| heterogeneous | `graphs/heterogeneous.yaml` | `jobs/heterogeneous.yaml` → `pagerank` |
| entity / event | `graphs/entity-event.json` | `jobs/entity-event-fastpath.yaml` → `fastpath` |
| multi-graph | `graphs/multi-graph.yaml` | `jobs/multi-graph.yaml` (two graphs in one run) |

Explicit graphs are `.json` (construct format); random graphs are `.yaml`
(`kind: random` specs). The `social-network.json` (fixed) and
`random-social-network.yaml` (generated) graphs share the `Person -KNOWS-> Person`
shape, so the social jobs run against either.

All jobs share one session (`gds-examples`), so runs reuse it; `--overwrite-graph`
replaces the projected graph each time.

### Projection kinds

A projection is either **remote** (a Cypher `query` returning
`gds.graph.project.remote(...)`) or **native** (selected by `node_labels` +
`relationship_types`, read straight from the DB). Give exactly one per projection.
`jobs/social-network-native.yaml` is the native counterpart of
`jobs/social-network-louvain.yaml`. Native projections also accept
`node_properties` / `relationship_properties` to load; both kinds accept
`undirected_relationship_types`.

## Run a job (social network)

Three steps — upload a graph, run the job, inspect:

```bash
# fixed graph
gds database upload -f graphs/social-network.json --overwrite
gds session  run --file jobs/social-network-louvain.yaml --overwrite-graph
gds database fetch --label Person        # community column populated

# same job, generated graph
gds database upload -f graphs/random-social-network.yaml --overwrite
gds session  run --file jobs/social-network-louvain.yaml --overwrite-graph
gds database fetch --label Person
```

The web-graph theme follows the same pattern (`--label Page`, `jobs/web-graph-pagerank.yaml`).

## Homogeneous graph

```bash
gds database upload -f graphs/homogeneous.yaml --overwrite
gds session  run --file jobs/homogeneous.yaml --overwrite-graph
gds database fetch
```

Clean up: `gds database delete --all`. Delete the session when done:
`gds session delete --file jobs/social-network-louvain.yaml`.
