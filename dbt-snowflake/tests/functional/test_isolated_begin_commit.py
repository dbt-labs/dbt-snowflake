import pytest
from dbt.tests.util import run_dbt_and_capture

my_model_sql = """
{{
    config(
        materialized = 'table',
        post_hook = '{{ my_silly_insert_macro() }}'
    )
}}
select 1 as id, 'blue' as color, current_timestamp as updated_at
"""

my_macro_sql = """
{% macro my_silly_insert_macro() %}
    {#-- This is a bad pattern! Made obsolete by changes in v0.21 + v1.2 --#}
    {% do run_query('begin;') %}
    {% set query %}
       insert into {{ this }} values (2, 'red', current_timestamp);
    {% endset %}
    {% do run_query(query) %}
    {% do run_query('commit;') %}
{% endmacro %}
"""


class TestModelWarehouse:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "my_macro.sql": my_macro_sql,
        }

    def test_isolated_begin_commit(self, project):
        # this should succeed / avoid raising an error
        results, log_output = run_dbt_and_capture(["run"])
        # but we should see a warning in the logs
        assert "WARNING" in log_output and "Explicit transactional logic" in log_output
