from collections.abc import Iterator

import pytest

from graphdatascience.progress.progress_bar import TqdmProgressBar


@pytest.fixture(autouse=True)
def _restore_tqdm_default_options() -> Iterator[None]:
    """Keep CLI tests from leaking tqdm's process-global default options.

    ``gds_cli.session.session_ops.connect()`` calls
    ``TqdmProgressBar.set_default_options({"leave": False})``, which mutates a
    class-level attribute shared across the whole process. Without a restore,
    that ``leave=False`` default bleeds into unrelated tests (notably the
    ``tests/unit/progress`` bar tests, which then see empty output because the
    bar erases itself on close). Snapshot and restore around each CLI test.
    """
    saved = dict(TqdmProgressBar._default_options)
    yield
    TqdmProgressBar._default_options = saved
