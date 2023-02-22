# graphdatascience documentation

This directory contains the source and related tooling of the [Neo4j Graph Data Science Client manual](https://neo4j.com/docs/graph-data-science-client).


## Setup

We use AsciiDoc for writing documentation, and we render it to both HTML and PDF.


### Rendering locally

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
[Asciidoctor](https://github.com/asciidoctor/asciidoctor) is used to parse the documentation and extract the Python code that should be tested.


### Installation

In addition to having followed the setup steps in [the contribution guidelines](../CONTRIBUTING.md#specifically-for-this-project) and having a Neo4j database with GDS installed that can be targeted in the testing, you need to:

 * Install Ruby
 * Install `bundler`:
   ```bash
   gem install bundler
   ```
 * Install the project's Ruby dependencies (from the `doc` directory):
   ```bash
   bundler install --gemfile tests/Gemfile
   ```
 * Install the version of the `graphdatascience` library that you want to test the docs against 


### Configuring

The tests will through the [Neo4j Python driver](https://neo4j.com/docs/python-manual/current/) connect to a Neo4j database based on the environment variables:

* `NEO4J_URI` (defaulting to "bolt://localhost:7687" if unset),
* `NEO4J_USER`,
* `NEO4J_PASSWORD` (defaulting to "neo4j" if unset),

However, if `NEO4J_USER` is not set the tests will try to connect without authentication.


### Running

Supposing that the targeted Neo4j database have been set up, running the documentation tests is a simple call (from the `doc` directory):

```bash
./tests/test_docs.rb python [-n test_community]
```

where `python` here refers to the Python interpreter that will be used to run example code.
Depending on your system, a different reference to an interpreter such as `python3` might be the right choice.
By adding the `-n test_community` option one can make sure that only tests that don't rely on GDS EE are run.


### Adding new tests

The example code snippets of the documentation that will be tested are those AsciiDoc blocks with style `source`, language `python` and without role `no-test`.
Further, if a block has a group attribute, then it will be concatenated with all other snippets of the same group into one script.
If a block has the enterprise attribute, it will only be run when the test `test_enterprise` is not filtered out.
If a block has the min-server-version attribute, it will only be run when the docs are tested against a GDS version >= min-server-version.

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

The script must be run from the project root directory and requires [Pandoc](https://pandoc.org/) to be already installed. The latest supported version of Pandoc is 2.19.2; version 3.0.1 seems to work the same but raises some warnings.

```bash
./scripts/nb2doc/convert.sh
```

### Style notes

For a successful conversion of the notebooks, some style notes apply.

* A notebook must only contain one first-level header, which should be in the first cell (as a title).
* TBD
