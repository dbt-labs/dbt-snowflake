import pytest

from dbt.tests.util import (
    run_dbt,
)

_MODELS__view = """
{{ config(
    materialized='view',
) }}

with dupes as (
    select 'foo' as key, 1 as value
    union all
    select 'foo' as key, 2 as value
)

select
    object_agg(key, value)
from dupes
"""


class TestRowValuesNotInExceptions:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": _MODELS__view}

    def test_row_values_were_scrubbed_from_duplicate_merge_exception(self, project):
        result = run_dbt(["run", "-s", "model"])
        assert len(result) == 1
        assert "Duplicate field key '[redacted]'" in result[0].message
