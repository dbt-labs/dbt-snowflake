import pytest

import json
from dbt.tests.util import run_dbt, run_dbt_and_capture

# Testing rationale:
# - snowflake SHOW TERSE SCHEMAS command returns at max 10K objects in a single call
# - when dbt attempts to write into a database with more than 10K schemas, compilation will fail
#   unless we paginate the result
# - however, testing this process is difficult at a full scale of 10K actual objects populated
#   into a fresh testing schema
# - accordingly, we create a smaller set of views and test the looping iteration logic in
#   smaller chunks

NUM_SCHEMAS = 100

TABLE_BASE_SQL = """
{{ config(materialized='table') }}

select 1 as id
""".lstrip()

MACROS__CREATE__TEST_SCHEMAS = """
{% macro create_test_schemas(database, schemas) %}

  {% for schema in schemas %}
    {% set sql %}
      use database {{database}};
      create schema if not exists {{schema}};
    {% endset %}

    {% do run_query(sql) %}
  {% endfor %}

{% endmacro %}
"""

MACROS__DROP__TEST_SCHEMAS = """
{% macro drop_test_schemas(database, schemas) %}

  {% for schema in schemas %}
    {% set sql %}
      drop schema {{database}}.{{schema}};
    {% endset %}

    {% do run_query(sql) %}
  {% endfor %}

{% endmacro %}
"""

MACROS__VALIDATE__SNOWFLAKE__LIST_SCHEMAS = """
{% macro validate_list_schemas(database, max_iter=11, max_results_per_iter=10) %}
    {% set schema_list_result = snowflake__list_schemas(database, max_iter=max_iter, max_results_per_iter=max_results_per_iter) %}
    {% set n_schemas = schema_list_result | length %}
    {{ log("n_schemas: " ~ n_schemas) }}
{% endmacro %}
"""

MACROS__VALIDATE__SNOWFLAKE__LIST_SCHEMAS_RAISE_ERROR = """
{% macro validate_list_schemas_raise_error(database) %}
    {{ snowflake__list_schemas(database, max_iter=33, max_results_per_iter=3) }}
{% endmacro %}
"""


def parse_json_logs(json_log_output):
    parsed_logs = []
    for line in json_log_output.split("\n"):
        try:
            log = json.loads(line)
        except ValueError:
            continue

        parsed_logs.append(log)

    return parsed_logs


def find_result_in_parsed_logs(parsed_logs, result_name):
    return next(
        (
            item["data"]["msg"]
            for item in parsed_logs
            if result_name in item["data"].get("msg", "msg")
        ),
        False,
    )


def find_exc_info_in_parsed_logs(parsed_logs, exc_info_name):
    return next(
        (
            item["data"]["exc_info"]
            for item in parsed_logs
            if exc_info_name in item["data"].get("exc_info", "exc_info")
        ),
        False,
    )


class TestListSchemasSingle:
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "validate_list_schemas.sql": MACROS__VALIDATE__SNOWFLAKE__LIST_SCHEMAS,
            "create_test_schemas.sql": MACROS__CREATE__TEST_SCHEMAS,
            "drop_test_schemas.sql": MACROS__DROP__TEST_SCHEMAS,
        }

    def test__snowflake__list_schemas_termination(self, project):
        """
        validates that we do NOT trigger pagination logic snowflake__list_relations_without_caching
        macro when there are fewer than max_results_per_iter relations in the target schema
        """

        database = project.database
        schemas = [f"test_schema_{i}" for i in range(0, NUM_SCHEMAS)]

        create_kwargs = {
            "database": database,
            "schemas": schemas,
        }

        run_dbt(["run-operation", "create_test_schemas", "--args", str(create_kwargs)])

        validate_kwargs = {"database": database, "max_iter": 1, "max_results_per_iter": 200}
        _, log_output = run_dbt_and_capture(
            [
                "--debug",
                "--log-format=json",
                "run-operation",
                "validate_list_schemas",
                "--args",
                str(validate_kwargs),
            ]
        )

        parsed_logs = parse_json_logs(log_output)
        n_schemas = find_result_in_parsed_logs(parsed_logs, "n_schemas")

        run_dbt(["run-operation", "drop_test_schemas", "--args", str(create_kwargs)])

        assert (
            n_schemas == f"n_schemas: {(NUM_SCHEMAS + 2)}"
        )  # include information schema and base test schema in the count


class TestListRelationsWithoutCachingFull:
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "validate_list_schemas.sql": MACROS__VALIDATE__SNOWFLAKE__LIST_SCHEMAS,
            "create_test_schemas.sql": MACROS__CREATE__TEST_SCHEMAS,
            "validate_list_schemas_raise_error.sql": MACROS__VALIDATE__SNOWFLAKE__LIST_SCHEMAS_RAISE_ERROR,
            "drop_test_schemas.sql": MACROS__DROP__TEST_SCHEMAS,
        }

    def test__snowflake__list_schemas(self, project):
        """
        validates pagination logic in snowflake__list_schemas macro counts
        the correct number of schemas in the target database when having to make multiple looped
        calls of SHOW TERSE SCHEMAS.
        """
        database = project.database
        schemas = [f"test_schema_{i}" for i in range(0, NUM_SCHEMAS)]

        create_kwargs = {"database": database, "schemas": schemas}

        run_dbt(["run-operation", "create_test_schemas", "--args", str(create_kwargs)])

        validate_kwargs = {"database": database}
        _, log_output = run_dbt_and_capture(
            [
                "--debug",
                "--log-format=json",
                "run-operation",
                "validate_list_schemas",
                "--args",
                str(validate_kwargs),
            ]
        )

        parsed_logs = parse_json_logs(log_output)
        n_schemas = find_result_in_parsed_logs(parsed_logs, "n_schemas")

        run_dbt(["run-operation", "drop_test_schemas", "--args", str(create_kwargs)])

        assert (
            n_schemas == f"n_schemas: {(NUM_SCHEMAS + 2)}"
        )  # include information schema and base test schema in the count

    def test__snowflake__list_schemas_raise_error(self, project):
        """
        validates pagination logic terminates and raises a compilation error
        when exceeding the limit of how many results to return.
        """
        run_dbt(["run"])

        database = project.database
        schemas = [f"test_schema_{i}" for i in range(0, NUM_SCHEMAS)]

        create_kwargs = {"database": database, "schemas": schemas}

        run_dbt(["run-operation", "create_test_schemas", "--args", str(create_kwargs)])

        validate_kwargs = {"database": database}
        _, log_output = run_dbt_and_capture(
            [
                "--debug",
                "--log-format=json",
                "run-operation",
                "validate_list_schemas_raise_error",
                "--args",
                str(validate_kwargs),
            ],
            expect_pass=False,
        )

        run_dbt(["run-operation", "drop_test_schemas", "--args", str(create_kwargs)])

        parsed_logs = parse_json_logs(log_output)
        traceback = find_exc_info_in_parsed_logs(parsed_logs, "Traceback")
        assert "dbt will list a maximum of 99 schemas in database" in traceback
