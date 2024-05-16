import os

import pytest

my_check = os.environ.get("DBT_PERFORMANCE_TESTING", 0)


def _get_setting(environment_variable: str) -> bool:
    raw_value = os.environ.get(environment_variable, False)
    return raw_value in [True, "True", "TRUE", 1, "1"]


performance_test = pytest.mark.skipif(
    not _get_setting("DBT_PERFORMANCE_TESTING"),
    reason=(
        "Performance test skipped, to turn on performance testing, "
        "please set the environment variable `DBT_PERFORMANCE_TESTING`"
    ),
)
