"""Sphinx configuration for loom-kernel documentation."""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

project = "loom-kernel"
author = "the-reacher-data"
copyright = f"{datetime.now().year}, {author}"
release = os.getenv("READTHEDOCS_VERSION", "latest")

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "sphinx_design",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "guides", "architecture", "examples-repo"]

html_theme = "furo"
html_static_path = ["_static"]
html_title = "loom-kernel docs"
html_css_files = [
    "custom.css",
]

autosummary_generate = True
autosummary_generate_overwrite = True
autosummary_imported_members = False
autodoc_typehints = "description"
autodoc_typehints_format = "short"
autodoc_member_order = "bysource"
autodoc_default_options = {
    "members": True,
    "show-inheritance": True,
}
autodoc_preserve_defaults = True
napoleon_google_docstring = True
napoleon_numpy_docstring = False


def _skip_duplicate_reexports(
    app: Any,
    what: str,
    name: str,
    obj: Any,
    skip: bool,
    options: Any,
) -> bool:
    """Skip known re-exported symbols that duplicate canonical API objects."""
    del what, obj, options
    current_module = app.env.temp_data.get("autodoc:module")
    duplicated_reexports = {
        ("loom.core.errors", "RuleViolation"),
        ("loom.core.errors", "RuleViolations"),
        ("loom.core.use_case", "RuleViolation"),
        ("loom.core.use_case", "RuleViolations"),
        ("loom.etl.observability", "OtelConfig"),
        ("loom.rest.model", "PaginationMode"),
    }
    if (current_module, name) in duplicated_reexports:
        return True
    return skip


source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

# Optional dependencies are mocked to keep docs builds lightweight and stable.
autodoc_mock_imports = [
    "aiocache",
    "celery",
    "deltalake",
    "fastapi",
    "fsspec",
    "kombu",
    "omegaconf",
    "polars",
    "pyarrow",
    "prometheus_client",
    "pydantic",
    "pyspark",
    "redis",
    "sqlalchemy",
    "starlette",
    "uvicorn",
]


def setup(app: Any) -> None:
    """Register Sphinx hooks."""
    app.connect("autodoc-skip-member", _skip_duplicate_reexports)
