import pytest
from dbt.tests.util import check_relations_equal, run_dbt
from tests.functional.adapter.statement_test.seeds import seeds_csv, statement_expected_csv

_STATEMENT_ACTUAL_SQL = """
-- {{ ref('seed') }}

{%- call statement('test_statement', fetch_result=True) -%}

  select
    count(*) as "num_records"

  from {{ ref('seed') }}

{%- endcall -%}

{% set result = load_result('test_statement') %}

{% set res_table = result['table'] %}
{% set res_matrix = result['data'] %}

{% set matrix_value = res_matrix[0][0] %}
{% set table_value = res_table[0]['num_records'] %}

select 'matrix' as source, {{ matrix_value }} as value
union all
select 'table' as source, {{ table_value }} as value
""".lstrip()


class TestStatements:
    @pytest.fixture(scope="class")
    def models(self):
        return {"statement_actual.sql": _STATEMENT_ACTUAL_SQL}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seeds_csv,
            "statement_expected.csv": statement_expected_csv,
        }

    def test_snowflake_statements(self, project):
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 2
        results = run_dbt()
        assert len(results) == 1

        db_with_schema = f"{project.database}.{project.test_schema}"
        check_relations_equal(
            project.adapter,
            [f"{db_with_schema}.STATEMENT_ACTUAL", f"{db_with_schema}.STATEMENT_EXPECTED"],
        )
