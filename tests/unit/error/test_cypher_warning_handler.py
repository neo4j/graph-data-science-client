import warnings

import pytest

from graphdatascience.caller_base import CallerBase
from graphdatascience.error.cypher_warning_handler import filter_id_func_deprecation_warning


class DummyCaller(CallerBase):
    def __init__(self) -> None:
        super().__init__(None, None, None)  # type: ignore

    @filter_id_func_deprecation_warning()
    def func_with_warning(self) -> str:
        warnings.warn(DeprecationWarning("The query used a deprecated function: `id`."))
        warnings.warn(
            DeprecationWarning(
                "The query used a deprecated function. ('id' has been replaced by 'elementId or an application-generated id')"
            )
        )
        warnings.warn(UserWarning("Some other warning"))
        return "done"


@pytest.mark.filterwarnings("ignore:Some other warning")
def test_filter_id_func_deprecation_warning_filters_specific_warnings() -> None:
    result = DummyCaller().func_with_warning()
    # Only the unrelated warning should be present
    assert result == "done"
