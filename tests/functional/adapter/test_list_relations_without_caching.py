import pytest

import json
from dbt.tests.util import run_dbt, run_dbt_and_capture

NUM_VIEWS = 100
NUM_EXPECTED_RELATIONS = 1 + NUM_VIEWS

TABLE_BASE_SQL = """
{{ config(materialized='table') }}

select 1 as id
""".lstrip()

VIEW_X_SQL = """
select id from {{ ref('my_model_base') }}
""".lstrip()

MACROS__VALIDATE__SNOWFLAKE__LIST_RELATIONS_WITHOUT_CACHING = """
{% macro validate_list_relations_without_caching(schema_relation) %}
    {% set relation_list_result = snowflake__list_relations_without_caching(schema_relation, max_iter=11, max_results_per_iter=10) %}
    {% set n_relations = relation_list_result | length %}
    {{ log("n_relations: " ~ n_relations) }}
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


class TestListRelationsWithoutCaching:
    @pytest.fixture(scope="class")
    def models(self):
        my_models = {"my_model_base.sql": TABLE_BASE_SQL}
        for view in range(0, NUM_VIEWS):
            my_models.update({f"my_model_{view}.sql": VIEW_X_SQL})

        return my_models

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "validate_list_relations_without_caching.sql": MACROS__VALIDATE__SNOWFLAKE__LIST_RELATIONS_WITHOUT_CACHING
        }

    def test__snowflake__list_relations_without_caching(self, project):
        # purpose of the first run is to create the replicated views in the target schema
        _ = run_dbt(["run"])

        # there is probably a better way to get the database and schema than this
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

            assert n_relations == "n_relations: 101"
