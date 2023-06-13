import pytest
from dbt.tests.util import check_relations_equal, run_dbt
from tests.functional.adapter.custom_schema_tests.seeds import seed_agg_csv, seed_csv

_VIEW_1_SQL = """
select * from {{ ref('seed') }}
""".lstrip()

_VIEW_2_SQL = """
{{ config(schema='custom') }}

select * from {{ ref('view_1') }}
""".lstrip()

_VIEW_3_SQL = """
{{ config(schema='test', materialized='table') }}


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


class TestCustomProjectSchemaWithPrefix:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seed_csv, "agg.csv": seed_agg_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"view_1.sql": _VIEW_1_SQL, "view_2.sql": _VIEW_2_SQL, "view_3.sql": _VIEW_3_SQL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"schema": "dbt_test"}}

    @pytest.fixture(scope="function")
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            alt_schema_list = ["DBT_TEST", "CUSTOM", "TEST"]
            for alt_schema in alt_schema_list:
                alt_test_schema = f"{project.test_schema}_{alt_schema}"
                relation = project.adapter.Relation.create(
                    database=project.database, schema=alt_test_schema
                )
                project.adapter.drop_schema(relation)

    def test__snowflake__custom_schema_with_prefix(self, project, clean_up):
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 2
        results = run_dbt()
        assert len(results) == 3

        db_with_schema = f"{project.database}.{project.test_schema}"
        check_relations_equal(
            project.adapter, [f"{db_with_schema}.SEED", f"{db_with_schema}_DBT_TEST.VIEW_1"]
        )
        check_relations_equal(
            project.adapter, [f"{db_with_schema}.SEED", f"{db_with_schema}_CUSTOM.VIEW_2"]
        )
        check_relations_equal(
            project.adapter, [f"{db_with_schema}.AGG", f"{db_with_schema}_TEST.VIEW_3"]
        )
