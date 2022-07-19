import pytest
from dbt.tests.fixtures.project import write_project_files


models__override_warehouse_sql = """
{{ config(snowflake_warehouse=env_var('SNOWFLAKE_TEST_ALT_WAREHOUSE', 'DBT_TEST_ALT'), materialized='table') }}
select current_warehouse() as warehouse

"""

models__expected_warehouse_sql = """
{{ config(materialized='table') }}
select '{{ env_var("SNOWFLAKE_TEST_ALT_WAREHOUSE", "DBT_TEST_ALT") }}' as warehouse

"""

models__invalid_warehouse_sql = """
{{ config(snowflake_warehouse='DBT_TEST_DOES_NOT_EXIST') }}
select current_warehouse() as warehouse

"""

project_config_models__override_warehouse_sql = """
{{ config(materialized='table') }}
select current_warehouse() as warehouse

"""

project_config_models__expected_warehouse_sql = """
{{ config(materialized='table') }}
select '{{ env_var("SNOWFLAKE_TEST_ALT_WAREHOUSE", "DBT_TEST_ALT") }}' as warehouse

"""


@pytest.fixture(scope="class")
def models():
    return {
        "override_warehouse.sql": models__override_warehouse_sql,
        "expected_warehouse.sql": models__expected_warehouse_sql,
        "invalid_warehouse.sql": models__invalid_warehouse_sql,
    }


@pytest.fixture(scope="class")
def project_config_models():
    return {
        "override_warehouse.sql": project_config_models__override_warehouse_sql,
        "expected_warehouse.sql": project_config_models__expected_warehouse_sql,
    }


@pytest.fixture(scope="class")
def project_files(
    project_root,
    models,
    project_config_models,
):
    write_project_files(project_root, "models", models)
    write_project_files(project_root, "project-config-models", project_config_models)
