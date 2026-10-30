# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
import os
import sys

from graphdatascience import __version__

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Neo4j Graph Data Science Python Client"
copyright = "2024, Neo4j"
author = "Neo4j"
version = __version__

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

# Add the root of the project to the path so that Sphinx can find the Python sources
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

extensions = [
    "sphinx.ext.autodoc",  # include docs from docstrings
    "sphinx.ext.intersphinx",  # link to other projects' documentation such as neo4j driver or pyArrow
    "sphinx.ext.napoleon",  # Support for NumPy and Google style docstrings
    "sphinxcontrib.pydantic",  # Support for Pydantic models
]

autodoc_class_signature = "separated"
autodoc_typehints = "both"  # show type-hints inferred from signature, skip in docstring.

# Treat unresolved cross-references as errors so broken links fail the build
# instead of silently rendering as plain text.
nitpicky = True

templates_path = ["_templates"]
exclude_patterns = []  # type: ignore

# -- Options for Sphinxcontrib Pydantic ------------------------------------------
sphinxcontrib_pydantic_model_show_json = False
sphinxcontrib_pydantic_settings_show_json = False
sphinxcontrib_pydantic_model_show_field_summary = False
sphinxcontrib_pydantic_model_show_validator_summary = False
sphinxcontrib_pydantic_model_show_private_members = False
sphinxcontrib_pydantic_field_show_alias = False
sphinxcontrib_pydantic_field_show_required = False
sphinxcontrib_pydantic_field_show_default = False
sphinxcontrib_pydantic_field_show_constraints = False

# Internal plumbing (pydantic validators, the write protocol) is not user-facing
# API; don't warn when rendered signatures reference it.
nitpick_ignore = [
    (
        "py:obj",
        "graphdatascience.procedure_surface.api.catalog.catalog_endpoints.GraphGenerationStats.check_empty_property",
    ),
    (
        "py:obj",
        "graphdatascience.procedure_surface.api.catalog.relationships_endpoints.RelationshipsWriteResult.coerce_relationship_properties",
    ),
    (
        "py:obj",
        "graphdatascience.embedding.config.GraphSAGEConfig.depth_validator",
    ),
    ("py:class", "graphdatascience.procedure_surface.api.write_protocol.WriteProtocol"),
    ("py:class", "graphdatascience.session.graph_mappings_api.GraphMappings"),
    # Internal types referenced by annotations of fields/members that are now
    # documented individually (previously hidden behind autodoc_pydantic).
    ("py:class", "pyarrow._flight.FlightMetadataReader"),
    ("py:class", "graphdatascience.session.session_lifecycle_manager.LifecycleManager"),
    ("py:class", "graphdatascience.session.aura_api.AuraApi"),
    ("py:class", "graphdatascience.session.aura_api_responses.SessionDetails"),
    ("py:class", "graphdatascience.session.aura_api_responses.SessionDetailsWithErrors"),
    ("py:class", "graphdatascience.query_runner.neo4j_query_runner.Neo4jQueryRunner"),
    ("py:class", "graphdatascience.arrow_client.v2.api_types.StepProgress"),
]

# Remaining nitpicky warnings are bare builtins, pydantic/annotated-types field
# metadata, and prose tokens that napoleon mis-reads as type cross-references
nitpick_ignore_regex = [
    (
        "py:class",
        r"^(1\.0|0\.0|=|bool|None|datetime|timedelta|None = None|bool = False|(annotated_types\.)?(Gt|Ge|Lt|Len)|FieldInfo|NoneType)$",
    ),
]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# use neo4j theme, which extends neo4j docs css for sphinx

html_theme = "neo4j"
html_theme_path = ["themes"]

# Additional templates that should be rendered to pages, maps page names to
# template names.
html_additional_pages = {"gentree": "gentree.html"}


# 01-nav.js is a copy of a js file of the same name that is included in the docs-ui bundle
def setup(app):  # type: ignore
    app.add_js_file("https://neo4j.com/docs/assets/js/site.js", loading_method="defer")
    app.add_js_file("js/12-fragment-jumper.js", loading_method="defer")
    app.add_js_file("js/deprecated.js", loading_method="defer")


intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "neo4j": ("https://neo4j.com/docs/api/python-driver/current/", None),
    "dateutil": ("https://dateutil.readthedocs.io/en/stable/", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
    "pandas": ("https://pandas.pydata.org/docs/", None),
    "pyarrow": ("https://arrow.apache.org/docs/", None),
    "networkx": ("https://networkx.org/documentation/stable/", None),
}

rst_epilog = """
.. |api-version| replace:: {versionnum}
""".format(
    versionnum=version,
)
