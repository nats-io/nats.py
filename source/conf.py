# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('../..'))


# -- Project information -----------------------------------------------------

project = 'nats.py'
copyright = '2021-2024, The NATS Authors'
author = 'The NATS Authors'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.coverage',
    'sphinx.ext.napoleon',
    'sphinx_autodoc_typehints',
    'myst_parser',
    'sphinx.ext.viewcode'
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
# html_theme = 'alabaster'
html_theme = 'furo'
# html_theme = 'sphinx_rtd_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

source_suffix = {
    '.rst': 'restructuredtext',
    '.txt': 'markdown',
    '.md': 'markdown',
}

html_theme_options = {
  "external_links": [
      ("Github", "https://github.com/nats-io/nats.py"),
      ("NATS", "https://nats.io")
  ]
}

logo_icon = '_static/nats-icon-color.png'
html_logo = '_static/nats-icon-color.png'

autodoc_member_order = 'bysource'


# https://github.com/tox-dev/sphinx-autodoc-typehints?tab=readme-ov-file#options

# Display fully qualified class names (e.g., 'module.ClassName') if True; otherwise, show only 'ClassName'.
typehints_fully_qualified = False

# Add type information for all parameters, including those without existing documentation, if True.
always_document_param_types = False

# Use the '|' operator for 'Union' types (e.g., 'X | Y') as per PEP 604 if True; otherwise, use 'Union[X, Y]' syntax.
always_use_bars_union = False

# Include the return type in the ':rtype:' directive if no existing ':rtype:' is found when True.
typehints_document_rtype = True

# Document the return type in the ':rtype:' directive if True; otherwise, include it in the ':return:' directive if present.
typehints_use_rtype = True

# Determine how default parameter values are documented:
# - None: Do not add defaults.
# - 'comma': Add defaults after the type (e.g., 'param (int, default: 1) -- text').
# - 'braces': Add '(default: ...)' after the type, useful for numpydoc styles.
# - 'braces-after': Add '(default: ...)' at the end of the parameter documentation text.
typehints_defaults = 'comma'

# Simplify 'Optional[Union[A, B]]' to 'Union[A, B, None]' in the documentation if True.
simplify_optional_unions = True

# Use a custom function to format type hints if provided; otherwise, use the default formatter.
typehints_formatter = None

# Show type hints in function signatures if True.
typehints_use_signature = True

# Show return type annotations in function signatures if True.
typehints_use_signature_return = True