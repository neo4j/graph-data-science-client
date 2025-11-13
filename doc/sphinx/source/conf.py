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
    "enum_tools.autoenum",  # specialised autoclass for enums
    "sphinx.ext.napoleon",  # Support for NumPy and Google style docstrings
    "sphinxcontrib.autodoc_pydantic",  # Support for Pydantic models
]

autodoc_class_signature = "separated"
autodoc_typehints = "both"  # show type-hints inferred from signature, skip in docstring.

templates_path = ["_templates"]
exclude_patterns = []  # type: ignore

# -- Options for Autodoc Pydantic ------------------------------------------------
autodoc_pydantic_model_show_json = False
autodoc_pydantic_settings_show_json = False
autodoc_pydantic_model_show_config_summary = False
autodoc_pydantic_model_show_field_summary = False
autodoc_pydantic_field_show_alias = False
autodoc_pydantic_field_show_required = False
autodoc_pydantic_field_show_optional = False
autodoc_pydantic_field_show_default = False

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# use neo4j theme, which extends neo4j docs css for sphinx

html_theme = "neo4j"
html_theme_path = ["themes"]


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
    "nx": ("https://networkx.org/documentation/stable/", None),
}

rst_epilog = """
.. |api-version| replace:: {versionnum}
""".format(
    versionnum=version,
)
