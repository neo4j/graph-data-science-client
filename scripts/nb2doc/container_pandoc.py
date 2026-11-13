#!/usr/bin/env python3
"""Run nbconvert with pandoc from a container.

nbconvert's AsciiDoc filters (`convert_pandoc`, `markdown2asciidoc`) shell out to
the `pandoc` binary on PATH. A matching pandoc is awkward to install locally, so
this entry point patches those filters to run the pinned `pandoc/core` image
instead, then hands the command line straight to nbconvert.

Usage: `uv run python scripts/nb2doc/container_pandoc.py <nbconvert args>`
"""

from __future__ import annotations

import os
import subprocess
import sys
from typing import Any, Optional

# Single source of truth for the pandoc version used to render the docs.
# Newest version published as an official `pandoc/core` image; upstream may be ahead.
PANDOC_VERSION = "3.11"


def _image() -> str:
    return os.environ.get("PANDOC_IMAGE", f"pandoc/core:{PANDOC_VERSION}")


def container_pandoc(
    source: str,
    fmt: str,
    to: str,
    extra_args: Optional[list[str]] = None,
    encoding: str = "utf-8",
) -> str:
    """Drop-in for nbconvert.utils.pandoc.pandoc that runs pandoc in a container.

    pandoc reads the source from stdin and writes the result to stdout, so no
    volume mounts are needed. stderr is left attached to surface pandoc warnings.
    """
    cmd = ["docker", "run", "-i", "--rm", _image(), "-f", fmt, "-t", to]
    if extra_args:
        cmd.extend(extra_args)

    try:
        proc = subprocess.run(cmd, input=source.encode(encoding), stdout=subprocess.PIPE)
    except FileNotFoundError as err:
        raise RuntimeError(
            "Docker is required to render the docs (pandoc runs in a container), "
            "but the `docker` executable was not found on PATH."
        ) from err

    if proc.returncode != 0:
        raise RuntimeError(f"pandoc container failed (exit {proc.returncode}): {' '.join(cmd)}")

    return proc.stdout.decode(encoding, "replace").rstrip("\n")


def _patch_nbconvert() -> None:
    from nbconvert.filters import markdown as filters_markdown
    from nbconvert.filters import pandoc as filters_pandoc
    from nbconvert.utils import pandoc as utils_pandoc

    # Fail loudly if a future nbconvert renames or drops these hooks, instead of
    # silently falling back to a local pandoc that may not exist.
    targets: list[tuple[Any, str, Any]] = [
        (utils_pandoc, "pandoc", container_pandoc),
        (filters_pandoc, "pandoc", container_pandoc),
        # markdown2asciidoc compares this against 3.0 to pick the ATX heading flag.
        (filters_markdown, "get_pandoc_version", lambda: PANDOC_VERSION),
    ]
    for module, attr, replacement in targets:
        if not hasattr(module, attr):
            raise RuntimeError(
                f"nbconvert has no {module.__name__}.{attr} to patch; "
                "container_pandoc.py needs updating for this nbconvert version."
            )
        setattr(module, attr, replacement)


def main(argv: Optional[list[str]] = None) -> None:
    _patch_nbconvert()

    from nbconvert.nbconvertapp import main as nbconvert_main

    nbconvert_main(sys.argv[1:] if argv is None else argv)


if __name__ == "__main__":
    main()
