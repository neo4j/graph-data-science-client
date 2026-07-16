# graphdatascience documentation

This directory contains the source and related tooling of the [Neo4j Graph Data Science Client manual](https://neo4j.com/docs/graph-data-science-client).


## Setup

We use AsciiDoc for writing documentation, and we render it to both HTML and PDF.


### Rendering locally

#### The easy way

render both manual and api-ref docs

```bash
just render-docs
```

#### The hard way

First, you have to run `npm install`.
Second, you have to run `npm install @neo4j-antora/antora-page-roles --save`.
After having done this once, you needn't do it again.

To build and view the docs locally, you can use `npm run start`.


## Authoring

We use a few conventions for documentation source management:

1. Write one sentence per line.
   This keeps the source readable.
   A newline in the source has no effect on the produced content.
2. Use two empty lines ahead of a new section.
   This keeps the source more readable.


### A note on inline LaTeX: you can't

Currently, our toolchain cannot render LaTeX snippets into PDF (works for HTML tho!).
So we are unable to use it.

What you can do though is use _cursive_, `monospace` and `.svg` images for anything more complicated.
https://www.codecogs.com/latex/eqneditor.php is helpful for inputting LaTeX and outputting `.svg` images, or any other image format for that matter.
We seem to use `.svg` so maybe stick to that.


## Testing

Selected parts of the source code examples in the documentation are run as tests.
[Asciidoctor](https://github.com/asciidoctor/asciidoctor) is used to parse the documentation and extract the Python code that should be tested (see `tests/test_docs.rb`).


### Running (the easy way)

The `just doc-tests` target spins up a local Neo4j + GDS plugin via `docker compose` (from `scripts/test_envs/`), runs the snippet tests against it, and tears the container down afterwards:

```bash
# Community edition: runs only the community-safe snippets (no license required)
just doc-tests enterprise=false

# Enterprise edition: runs all scopes; requires a license at ${HOME}/.gds_license
just doc-tests enterprise=true
```


### Running (manually)

If you already have a Neo4j database with GDS running, you can invoke the Ruby harness directly.
You need to:

 * Install Ruby and `bundler` (`gem install bundler`)
 * Install the project's Ruby dependencies (from the `doc/tests` directory): `bundle install`
 * Install the version of the `graphdatascience` library that you want to test the docs against

The tests connect through the [Neo4j Python driver](https://neo4j.com/docs/python-manual/current/) based on the environment variables `NEO4J_URI` (default `bolt://localhost:7687`), `NEO4J_USERNAME`, and `NEO4J_PASSWORD` (default `neo4j`).
If `NEO4J_USERNAME` is not set the tests try to connect without authentication.

Then, from the `doc/tests` directory:

```bash
bundle exec ruby test_docs.rb $(uv run which python) [-n test_community]
```

where the argument is the Python interpreter used to run the example code.
Passing `-n test_community` runs only the snippets that don't rely on GDS Enterprise Edition.


### Deployment tabs

The manual documents each deployment mode as an Antora tabbed example (`[.include-with-Neo4j-server]` for the self-managed plugin, `[.include-with-Aura-Graph-Analytics]` for GDS Sessions, and `[.include-with-AuraDS]` for AuraDS).
The doc tests target the plugin/self-managed deployment only: `test_docs.rb` runs untabbed snippets and those in the `[.include-with-Neo4j-server]` tab, and skips snippets nested inside the Aura Graph Analytics and AuraDS tabs.


### Adding new tests

The example code snippets of the documentation that will be tested are those AsciiDoc blocks with style `source`, language `python` and without role `no-test`.
Further, if a block has a group attribute, then it will be concatenated with all other snippets of the same group into one script.
If a block has the enterprise attribute, it will only be run when the test `test_enterprise` is not filtered out.
If a block has the min-server-version attribute, it will only be run when the docs are tested against a GDS version >= min-server-version.
Snippets inside a deployment tab are only run in the matching deployment lane (see [Deployment tabs](#deployment-tabs)); to iterate on a single page, set `DOC_TEST_FILE=<substring>`.
The harness logs per-file progress to stderr as it runs; set `DOC_TEST_LOGLEVEL=DEBUG` for per-script logging (with timings).

Additionally, before a code snippet from the documentation is run, it is:

* Prepended with some setup code, such as creating a `GraphDataScience` object named `gds` which is set up based on the [configuration](#configuring),
* Extended with some clean up code, such as dropping all projected graphs and reseting the database.

Please inspect the test script `tests/test_docs.rb` for more details.


### Code style

To enforce Ruby code style of the testing source we use [RuboCop](https://github.com/rubocop/rubocop).
It should be installed by the command `bundler install --gemfile tests/Gemfile` [above](#installation).

To use RuboCop for linting simply call `rubocop tests`, and for enforcing rules (formatting) one can call it with the `-A` option.

Our custom RuboCop configuration can be found in `tests/.rubocop.yml`.


## Generate documentation from Jupyter notebooks

The documents in the `tutorials` section are automatically generated from the Jupyter notebooks.

The script `../scripts/nb2doc/convert.sh` can be used:

* to generate documents from new notebooks;
* to ensure that any changes to the existing notebooks are reflected in the existing documents (for instance in a CI setting).

The script must be run from the project root directory and requires [Pandoc](https://pandoc.org/) to be already installed. The latest supported version of Pandoc is 3.6.2

```bash
./scripts/nb2doc/convert.sh
```


### Style notes

For a successful conversion of the notebooks, some style notes apply.

* A notebook must only contain one first-level header, which should be in the first cell (as a title).
* The notebook should contain an "Open with Colab" badge after its title and before any other section (instructions below)
* The beginning of the introduction section of the notebook (after the Colab badge) should reference where the notebook is hosted. See existing notebooks for examples.
* TBD


### Adding an "Open with Colab" badge

1. Go to https://openincolab.com/
2. Insert the Github link to the notebook and press "GENERATE"
3. Paste the generated HTML into a new "Markdown" cell in the notebook after the title cell
4. Lastly, edit the raw notebook file by adding `"colab_type": "text"` to the `metadata` map of the cell containing the Colab badge

Please refer to other notebooks for examples on the final result.
