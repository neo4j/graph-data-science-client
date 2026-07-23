# `gds` CLI — architecture

> Experimental, development-only tool. Not part of the supported `graphdatascience` API.

This document explains how the `gds` CLI interprets a job-config YAML and what it
does behind the scenes, then maps out each component. Start with the high-level
summary; drill into the detailed sections as needed.

---

## 1. How a job config is interpreted (high level)

A job config is a single YAML document with two top-level keys:

- `session` — the managed Aura GDS session to run against (`memory` / `ttl` — minutes
  or a `30m`/`2h`/`1d` duration, an optional `name`, and optional `cloud` + `region`
  for a **standalone** session not attached to any database).
- `jobs` — an ordered list of **jobs**. Each job is a self-contained unit of work:
  one `project` (a graph), an ordered list of `compute` steps, and `mutate` / `write`
  selections over the results.

Credentials are **never** in the config — they come from environment variables.

### What `gds run` does, per job (in order)

1. **Project** the job's graph into the session. The config has no graph name, so
   the CLI assigns an internal catalog name `job-<i>` (0-based). Projection is
   `cypher` (a remote Cypher query returning `gds.graph.project.remote(...)`),
   `native` (select by node labels + relationship types) — both read from the
   attached database — or `construct` (built from a graph `file` via
   `gds.graph.construct`, the only kind a standalone session can use).
2. **Compute** each algorithm in listed order. Each `compute` calls the client's
   `<algorithm>.compute(graph, **params)`, which starts the algorithm and returns a
   **`JobHandle`** — a reference to the result held in the session, not yet written
   anywhere. Parameters are camelCase in the config and collapsed to snake_case for
   the client call; `resultProperty` (the produced property name) is not a compute
   parameter.
3. **Mutate (materialize)** — a produced `resultProperty` is written into the
   **in-session graph** *immediately*, in compute order, when it needs to be there
   for a later step. Materialization is **auto-derived**: a property is mutated if a
   later compute names it as an input (e.g. FastRP's `featureProperties`). An explicit
   `mutate` list is an optional override to force extra properties.
4. **Write (persist)** — each `write` entry persists a produced property. For an
   **attached** session it goes to the **Aura database**, two paths, same result:
   - property **was materialized** → graph-level writeback
     `gds.graph.node_properties.write` (supports `writeProperty` rename);
   - property **was only computed** (not materialized) → written **directly from the
     compute handle** (`handle.write(...)`), skipping mutate to save session memory.

   For a **standalone** session (no database) there is nothing to write back to, so
   each write is instead **streamed to its `outputFile`** (relative to the job config;
   CSV or JSON by extension; properties sharing a file are written together).
5. **Drop** the job's graph, then move to the next job.

### `mutate` vs `write` — the mental model

- **mutate** = keep the result *in the session graph* (in memory). Needed when a
  later compute reads it as a feature, or as the staging step before a graph-level
  writeback. Nothing reaches the database.
- **write** = persist the result — to the *Aura database* (attached) or a *file*
  (standalone). An attached write of a materialized property reads it back out of the
  graph; a write of a compute-only property streams straight from the finished compute
  job (memory-saving fast path).
- A property referenced by a later compute is materialized **automatically** (no need
  to list it under `mutate`); `mutate` remains as an explicit override. A terminal
  output that's only persisted needs neither.

### Validation

Every config is validated twice before execution: first against a JSON Schema
(Draft 2020-12), then parsed into pydantic models (which add cross-field checks).
The **per-job** schema (`job-spec.schema.json`) is the same document the
`gds-jobs-api` Go server validates a single job against.

---

## 2. Components at a glance

- **`app.py`** — root Typer app; wires the entry points: `gds run`,
  `gds database …` (alias `gds db …`), and `gds sessions …`, and prints the banner.
- **`run.py`** — the `gds run` command: load config(s) → resolve session name
  (`--session-name` → config `session.name` → throwaway) → connect → resolve
  graph-overwrite → execute → delete session unless it was named/kept.
- **`common/`** — shared bits: `env.py` (the unified environment set → connection
  objects) and console width.
- **`session/`** — everything about running a job config against a session:
  - `config.py` — the pydantic model + JSON-schema validation of the config.
  - `schema/` — the two JSON Schemas (CLI input + per-job spec).
  - `session_ops.py` — create / reconnect / list / delete the Aura session.
  - `steps.py` — the execution engine (`run_all`) implementing §1.
  - `algorithms.py` — map algorithm names to client endpoints + param conversion.
  - `report.py` — live rich-console progress + a timing/size summary table.
  - `commands.py` — the `gds sessions` sub-app (create / list / delete) plus the
    shared `_load`/`_connect` helpers used by `gds run`.
- **`database/`** — the `gds database` command group: generate synthetic graphs and
  upload/fetch/delete them via the raw Neo4j driver (test-data tooling, independent
  of the session flow).

---

## 3. Component details

### `app.py` — root command
- Builds the top-level Typer app and registers `run` (top-level command) plus the
  `database` and `sessions` sub-apps. A custom `_AliasGroup` (a `TyperGroup`) resolves
  `db` → `database` at dispatch, so `gds db …` works but `--help` lists `database` once.
- `@app.callback` prints the "EXPERIMENTAL" banner to stderr on every invocation
  (not on `--help`), so stdout stays clean.
- Silences the SDK's `UserWarning`s (e.g. FastPath's preview notice) so the CLI's
  own reporting is the single source of truth.

### `run.py` — `gds run`
- Reads config(s) from `--file`/`-f` (repeatable) or, if omitted, the `$GDS_JOB_CONFIG`
  env var (lets a k8s Job carry its config inline instead of a ConfigMap + volume mount).
- Each `-f` is a file or a **directory**; `_load_configs` expands directories to their
  sorted `*.yaml`/`*.yml` members, de-duplicates, and preserves order. `_run_one_config`
  then runs each config **independently** (its own connect → execute → delete cycle,
  from its own `session:` block) — exactly like invoking `gds run -f <file>` once per
  file. No shared session, so a run can freely mix standalone and attached configs;
  multiple configs are separated by a centered, double-lined header.
- `_resolve_overwrite`: before executing, checks whether any `job-<i>` graph is
  already in the session catalog (e.g. left over from an interrupted run). With
  `--overwrite-graph` it drops unconditionally; otherwise it prompts to drop and
  re-project, aborting if declined.
- Resolves the session name by precedence `--session-name` → config `session.name`
  → a generated `cli-<uuid4>`. A named session (either source) is kept; the anonymous
  throwaway is deleted once the run completes (handy for one-off k8s Jobs).
- Calls `steps.run_all`, deleting the throwaway session in a `finally` so an
  interrupted run doesn't leak it.

### `session/config.py` — config model + validation
- Pydantic v2 models: `JobsConfig` (`session` + `jobs`), `JobSpec`
  (`project`/`compute`/`mutate`/`write`), `ProjectSpec`, `ComputeSpec`,
  `MutateSpec`, `WriteSpec`, `SessionConfig`.
- Job-spec fields use camelCase aliases (YAML stays camelCase, Python attrs are
  snake_case); the `session` block keeps snake_case. `session.ttl` accepts minutes
  (int) or a `30m`/`2h`/`1d` string and is parsed to a `timedelta` (`parse_ttl`).
- `_from_data` validates against `job-config.schema.json` first, then
  `model_validate`. Cross-field validators enforce: projection kind (cypher / native /
  construct), a required `resultProperty` per compute, that every `mutate`/`write`
  `nodeProperty` is actually produced by some compute, and the standalone rules — a
  standalone session (`cloud`+`region`, both-or-neither) requires `construct`
  projections and an `outputFile` per write, while an attached session rejects
  `outputFile`. A legacy flat config (`projections`/`algorithms`) is rejected with a
  clear message.
- `SessionConfig.name` is optional. `JobSpec.mutated_properties` is the union of the
  explicit `mutate` list and the auto-derived set (a produced property consumed as an
  input by a later compute — see `algorithms.input_property_references`).
- `WriteSpec.target` resolves the output name (`writeProperty`/DB column or, by
  default, `nodeProperty`).

### `session/schema/`
- `job-config.schema.json` — the CLI input schema (`session` + `jobs`), embedding
  the per-job definition via `$defs`.
- `job-spec.schema.json` — the standalone per-job schema; mirrors what the
  `gds-jobs-api` Go server validates, so a job validates identically on both sides.

### `session/session_ops.py` — session lifecycle
- `build_sessions()` → `GdsSessions(AuraAPICredentials from env)`.
- `connect(cfg, name)` — idempotent `get_or_create` for a `SessionConfig` under the
  given name (with a short retry for transient "please retry"/not-found errors), then
  `verify_connectivity()`. Attached sessions pass a `DbmsConnectionInfo` (from the
  env); standalone sessions pass a `CloudLocation(cloud, region)` and no db connection.
  The projected graph lives server-side in the session catalog, keyed by name.
- `find_session` / `list_sessions` / `delete` — best-effort lookup, listing, and
  deletion by name.

### `session/steps.py` — execution engine
- `run_all(gds, cfg, overwrite_graph, report, base_dir)` iterates jobs, calling
  `_run_one_job` per job and aggregating `{computes, writes}`. `base_dir` (the config
  file's directory) is where a `construct` `file` and standalone `outputFile`s resolve.
- `_run_one_job`: project (`_project_one`, with a `construct` branch that builds the
  graph from a file via `_construct_from_file`) → for each compute `_run_one_compute`
  (compute handle, and `handle.mutate` immediately if the property is in
  `mutated_properties`) → persist. Attached: `_write_back_mutated` for materialized
  properties, `_write_direct` for compute-only. Standalone: writes are grouped by
  `outputFile` and streamed together via `_stream_group`. Then `_drop_one`.
- `_write_direct` calls `handle.wait()` before `handle.write(...)` — the compute job
  must have finished (its result in the session result store) before the remote
  write-back fetches it, otherwise the DB reports "No entry with job id … in result
  store".
- `job_graph_name(i)` → the deterministic internal name `job-<i>`.

### `session/algorithms.py` — algorithm dispatch
- `ALGORITHM_ATTR` maps a canonical (lowercased, alnum-only) algorithm name to a gds
  client endpoint attribute (e.g. `pagerank` → `page_rank`, `fastpath` → `fast_path`).
- `to_snake_case` / `to_snake_params` collapse camelCase config keys to the
  snake_case kwargs the client's Python methods expect (the client only ships the
  reverse conversion).
- `ALGORITHM_INPUT_PROPERTY_KEYS` + `input_property_references(algorithm, config)`
  name the config keys that reference *input* node properties (e.g. FastRP's
  `featureProperties`, WCC's `seedProperty`), which is how `mutated_properties`
  auto-derives what must be materialized before a later compute.
- `compute_algorithm(gds, graph, spec)` resolves the endpoint and calls
  `endpoint.compute(graph, **snake_params)` → `JobHandle` (works uniformly for every
  algorithm, including FastPath).

### `session/report.py` — reporting
- `JobReport` renders labeled phase sections, a live line per step with elapsed
  time, and a final size/timing summary table (rich). `quiet=True` suppresses
  output while still accumulating stats (used by tests and `--no-progress-bar`).

### `session/commands.py` — `gds sessions` + shared helpers
- The `sessions` sub-app: `create` (create/reconnect), `list`, `delete` (with a
  confirmation prompt, `--yes` to skip). `create`/`delete` resolve their session
  from a config file (`-f`) and/or CLI flags (`--name`/`--memory`/`--ttl`/`--cloud`/
  `--region`), CLI overriding the config, via `_resolve_session`/`_resolve_session_name`.
- Shared helpers used here and by `gds run`: `_load` (dotenv + parse `JobsConfig`
  from file or `$GDS_JOB_CONFIG`) and `_connect` (idempotent connect for a
  `SessionConfig` + name, recreating a session stuck in a "deleting" state).

### `common/env.py` — the unified environment set
- One env set drives every command. `load_env` layers dotenv files under real env
  vars (real env always wins).
- Produces the object each flow needs from the same variables: `DatabaseConfig`
  (raw `neo4j` driver, for `gds database`), `DbmsConnectionInfo` + `AuraAPICredentials`
  (for the session flow). `AURA_INSTANCEID` is derived from `NEO4J_URI` if unset.
- Keys: `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`, `NEO4J_DATABASE`,
  `AURA_INSTANCEID`, `CLIENT_ID`, `CLIENT_SECRET`, `PROJECT_ID`.

### `database/` — test-data tooling (`gds database`, alias `gds db`)
- `commands.py` — `upload`, `summary`, `fetch`, `delete` against a Neo4j/Aura DB via
  the raw driver. Independent of the session flow; shares only `common/env.py`.
  `delete` previews the counts (a dry-run) and prompts for confirmation before
  removing anything (`--yes` skips the prompt).
- `db.py` `open_driver` — the single place drivers are opened, with server
  notifications disabled (`notifications_min_severity="OFF"`) so probing queries on
  a fresh DB (e.g. `MATCH (n:Dev) ...` before any upload) don't leak `label does not
  exist` warnings.
- `graph/` — synthetic graph generation: `random_graph.py` (config +
  `create_graph`), `random_edges.py` (Uniform/PowerLaw degree), `random_data.py`
  (property generators), `graph.py` (`Graph`/`NodeIdMapping`).
- `construct.py` / `spec.py` — build a `Graph` from a JSON/YAML spec file.
- `upload.py` — batched `execute_write` uploader (dynamic labels/types, elementId
  join, overwrite guard). `fetch.py` — read-back + property summaries. `db.py` —
  `DatabaseClient`. `output.py` — formatting helpers.

---

## 4. Data flow (one `gds run`)

```
YAML ─► config.JobsConfig            (JSON-Schema + pydantic validation)
        │
run.py ─► session_ops.connect        (get_or_create; attached=db_connection,
        │                                             standalone=cloud_location)
steps.run_all ─► per job:
        project (job-<i>)  ────────────────────────────────┐  cypher / native (DB), or
        for each compute:                                  │  construct (from file)
            endpoint.compute(...) ─► JobHandle             │  algorithms.compute_algorithm
            if auto-derived/mutate: handle.mutate() ◄──────┘  (name→endpoint, camel→snake)
        persist writes:
            attached, materialized:  graph.node_properties.write(...)  (DB)
            attached, compute-only:  handle.wait(); handle.write(...)  (DB, direct)
            standalone:              stream grouped by outputFile      (CSV/JSON file)
        drop (job-<i>)
        │
report.JobReport ─► live progress + summary table
```
