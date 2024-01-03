import pytest

from dbt.tests.util import (
    run_dbt,
)

_MODELS__incremental_model = """
{{ config(
    materialized='incremental',
    unique_key='id'
) }}

with data as (
    SELECT $1 id, $2 name FROM (
        VALUES (1, 'one'), (2, 'two'), (3, 'three'), (1, 'one')
    )
)
select * from data
"""


class TestRowValuesNotInExceptions:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": _MODELS__incremental_model}

    def test_row_values_were_scrubbed_from_duplicate_merge_exception(self, project):
        result = run_dbt(["run", "-s", "model"])
        assert len(result) == 1

        result = run_dbt(["run", "-s", "model"], expect_pass=False)
        assert len(result) == 1
        assert "Row Values: [redacted]" in result[0].message
        assert "'one'" not in result[0].message
