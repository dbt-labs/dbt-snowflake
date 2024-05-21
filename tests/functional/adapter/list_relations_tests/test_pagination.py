import os

import pytest

import json
from dbt.tests.util import run_dbt, run_dbt_and_capture

# Testing rationale:
# - snowflake SHOW TERSE OBJECTS command returns at max 10K objects in a single call
# - when dbt attempts to write into a schema with more than 10K objects, compilation will fail
#   unless we paginate the result
# - however, testing this process is difficult at a full scale of 10K actual objects populated
#   into a fresh testing schema
# - accordingly, we create a smaller set of views and test the looping iteration logic in
#   smaller chunks

NUM_VIEWS = 90
NUM_DYNAMIC_TABLES = 10
# the total number should be between the numbers referenced in the "passing" and "failing" macros below
# - MACROS__VALIDATE__SNOWFLAKE__LIST_RELATIONS_WITHOUT_CACHING (11 iter * 10 results per iter -> 110 objects)
# - MACROS__VALIDATE__SNOWFLAKE__LIST_RELATIONS_WITHOUT_CACHING_RAISE_ERROR (33 iter * 3 results per iter -> 99 objects)
NUM_EXPECTED_RELATIONS = 1 + NUM_VIEWS + NUM_DYNAMIC_TABLES

TABLE_BASE_SQL = """
{{ config(materialized='table') }}

select 1 as id
""".lstrip()

VIEW_X_SQL = """
select id from {{ ref('my_model_base') }}
""".lstrip()

DYNAMIC_TABLE = (
    """
{{ config(
    materialized='dynamic_table',
    target_lag='1 hour',
    snowflake_warehouse='"""
    + os.getenv("SNOWFLAKE_TEST_WAREHOUSE")
    + """',
) }}

select id from {{ ref('my_model_base') }}
"""
)

MACROS__VALIDATE__SNOWFLAKE__LIST_RELATIONS_WITHOUT_CACHING = """
{% macro validate_list_relations_without_caching(schema_relation) %}
    {% set relation_list_result = snowflake__list_relations_without_caching(schema_relation, max_iter=11, max_results_per_iter=10) %}
    {% set n_relations = relation_list_result | length %}
    {{ log("n_relations: " ~ n_relations) }}
{% endmacro %}
"""

MACROS__VALIDATE__SNOWFLAKE__LIST_RELATIONS_WITHOUT_CACHING_RAISE_ERROR = """
{% macro validate_list_relations_without_caching_raise_error(schema_relation) %}
    {{ snowflake__list_relations_without_caching(schema_relation, max_iter=33, max_results_per_iter=3) }}
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


class TestListRelationsWithoutCachingSingle:
    @pytest.fixture(scope="class")
    def models(self):
        my_models = {"my_model_base.sql": TABLE_BASE_SQL}
        for view in range(0, NUM_VIEWS):
            my_models.update({f"my_model_{view}.sql": VIEW_X_SQL})
        for dynamic_table in range(0, NUM_DYNAMIC_TABLES):
            my_models.update({f"my_dynamic_table_{dynamic_table}.sql": DYNAMIC_TABLE})
        return my_models

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "validate_list_relations_without_caching.sql": MACROS__VALIDATE__SNOWFLAKE__LIST_RELATIONS_WITHOUT_CACHING,
        }

    def test__snowflake__list_relations_without_caching_termination(self, project):
        """
        validates that we do NOT trigger pagination logic snowflake__list_relations_without_caching
        macro when there are fewer than max_results_per_iter relations in the target schema
        """
        run_dbt(["run", "-s", "my_model_base"])

        database = project.database
        schemas = project.created_schemas

        for schema in schemas:
            schema_relation = f"{database}.{schema}"
            kwargs = {"schema_relation": schema_relation}
            _, log_output = run_dbt_and_capture(
                [
                    "--debug",
                    "--log-format=json",
                    "run-operation",
                    "validate_list_relations_without_caching",
                    "--args",
                    str(kwargs),
                ]
            )

            parsed_logs = parse_json_logs(log_output)
            n_relations = find_result_in_parsed_logs(parsed_logs, "n_relations")

            assert n_relations == "n_relations: 1"


class TestListRelationsWithoutCachingFull:
    @pytest.fixture(scope="class")
    def models(self):
        my_models = {"my_model_base.sql": TABLE_BASE_SQL}
        for view in range(0, NUM_VIEWS):
            my_models.update({f"my_model_{view}.sql": VIEW_X_SQL})
        for dynamic_table in range(0, NUM_DYNAMIC_TABLES):
            my_models.update({f"my_dynamic_table_{dynamic_table}.sql": DYNAMIC_TABLE})
        return my_models

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "validate_list_relations_without_caching.sql": MACROS__VALIDATE__SNOWFLAKE__LIST_RELATIONS_WITHOUT_CACHING,
            "validate_list_relations_without_caching_raise_error.sql": MACROS__VALIDATE__SNOWFLAKE__LIST_RELATIONS_WITHOUT_CACHING_RAISE_ERROR,
        }

    def test__snowflake__list_relations_without_caching(self, project):
        """
        validates pagination logic in snowflake__list_relations_without_caching macro counts
        the correct number of objects in the target schema when having to make multiple looped
        calls of SHOW TERSE OBJECTS.
        """
        # purpose of the first run is to create the replicated views in the target schema
        run_dbt(["run"])

        database = project.database
        schemas = project.created_schemas

        for schema in schemas:
            schema_relation = f"{database}.{schema}"
            kwargs = {"schema_relation": schema_relation}
            _, log_output = run_dbt_and_capture(
                [
                    "--debug",
                    "--log-format=json",
                    "run-operation",
                    "validate_list_relations_without_caching",
                    "--args",
                    str(kwargs),
                ]
            )

            parsed_logs = parse_json_logs(log_output)
            n_relations = find_result_in_parsed_logs(parsed_logs, "n_relations")

            assert n_relations == f"n_relations: {NUM_EXPECTED_RELATIONS}"

    def test__snowflake__list_relations_without_caching_raise_error(self, project):
        """
        validates pagination logic terminates and raises a compilation error
        when exceeding the limit of how many results to return.
        """
        run_dbt(["run"])

        database = project.database
        schemas = project.created_schemas

        for schema in schemas:
            schema_relation = f"{database}.{schema}"
            kwargs = {"schema_relation": schema_relation}
            _, log_output = run_dbt_and_capture(
                [
                    "--debug",
                    "--log-format=json",
                    "run-operation",
                    "validate_list_relations_without_caching_raise_error",
                    "--args",
                    str(kwargs),
                ],
                expect_pass=False,
            )

            parsed_logs = parse_json_logs(log_output)
            traceback = find_exc_info_in_parsed_logs(parsed_logs, "Traceback")
            assert "dbt will list a maximum of 99 objects in schema " in traceback
