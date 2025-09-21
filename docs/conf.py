# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------

project = 'EMHealth'
copyright = '2023-2025, Grigory Sharov'
author = 'Grigory Sharov'

# The full version, including alpha/beta/rc tags
release = '0.1a2'

# -- General configuration ---------------------------------------------------
# If your documentation needs a minimal Sphinx version, state it here.
needs_sphinx = '8.0'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = []

# Add any paths that contain templates here, relative to this directory.
source_suffix = {'.rst': 'restructuredtext'}
master_doc = 'index'
language = 'en'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build']

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_static_path = ['_static']
html_theme = 'sphinx_rtd_theme'
html_theme_options = {'sticky_navigation': False, 'collapse_navigation': False, 'navigation_depth': 2}
