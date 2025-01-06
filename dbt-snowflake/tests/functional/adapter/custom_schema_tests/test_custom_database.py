import pytest
import os
from dbt.tests.util import check_relations_equal, check_table_does_exist, run_dbt
from tests.functional.adapter.custom_schema_tests.seeds import seed_agg_csv, seed_csv

_VIEW_1_SQL = """
select * from {{ ref('seed') }}
""".lstrip()

_VIEW_2_SQL = """
{{ config(database='alt') }}
select * from {{ ref('view_1') }}
""".lstrip()

_VIEW_3_SQL = """
{{ config(database='alt', materialized='table') }}


with v1 as (

    select * from {{ ref('view_1') }}

),

v2 as (

    select * from {{ ref('view_2') }}

),

combined as (

    select last_name from v1
    union all
    select last_name from v2

)

select
    last_name,
    count(*) as count

from combined
group by 1
""".lstrip()

_CUSTOM_DB_SQL = """
{% macro generate_database_name(database_name, node) %}
    {% if database_name == 'alt' %}
        {{ env_var('SNOWFLAKE_TEST_ALT_DATABASE') }}
    {% elif database_name %}
        {{ database_name }}
    {% else %}
        {{ target.database }}
    {% endif %}
{% endmacro %}
""".lstrip()

ALT_DATABASE = os.getenv("SNOWFLAKE_TEST_ALT_DATABASE")


class TestOverrideDatabase:
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "custom_db.sql": _CUSTOM_DB_SQL,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seed_csv, "agg.csv": seed_agg_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_1.sql": _VIEW_1_SQL,
            "view_2.sql": _VIEW_2_SQL,
            "view_3.sql": _VIEW_3_SQL,
        }

    @pytest.fixture(scope="function")
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=ALT_DATABASE, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    def test_snowflake_override_generate_db_name(self, project, clean_up):
        seed_results = run_dbt(["seed", "--full-refresh"])
        assert len(seed_results) == 2

        db_with_schema = f"{project.database}.{project.test_schema}"
        alt_db_with_schema = f"{ALT_DATABASE}.{project.test_schema}"
        seed_table = "SEED"
        agg_table = "AGG"
        view_1 = "VIEW_1"
        view_2 = "VIEW_2"
        view_3 = "VIEW_3"

        check_table_does_exist(project.adapter, f"{db_with_schema}.{seed_table}")
        check_table_does_exist(project.adapter, f"{db_with_schema}.{agg_table}")

        results = run_dbt()
        assert len(results) == 3

        check_table_does_exist(project.adapter, f"{db_with_schema}.{view_1}")
        check_table_does_exist(project.adapter, f"{alt_db_with_schema}.{view_2}")
        check_table_does_exist(project.adapter, f"{alt_db_with_schema}.{view_3}")

        # not overridden
        check_relations_equal(
            project.adapter, [f"{db_with_schema}.{seed_table}", f"{db_with_schema}.{view_1}"]
        )

        # overridden
        check_relations_equal(
            project.adapter, [f"{db_with_schema}.{seed_table}", f"{alt_db_with_schema}.{view_2}"]
        )
        check_relations_equal(
            project.adapter, [f"{db_with_schema}.{agg_table}", f"{alt_db_with_schema}.{view_3}"]
        )
