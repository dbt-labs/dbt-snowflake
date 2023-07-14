from dataclasses import replace
from typing import Type

import pytest

from dbt.exceptions import DbtRuntimeError

from dbt.adapters.snowflake.relation.models import (
    SnowflakeDynamicTableRelation,
    SnowflakeDynamicTableRelationChangeset,
    SnowflakeDynamicTableTargetLagRelation,
)


@pytest.mark.parametrize(
    "config_dict,exception",
    [
        (
            {
                "name": "my_indexed_dynamic_table",
                "schema": {
                    "name": "my_schema",
                    "database": {"name": "my_database"},
                },
                "query": "select 42 from meaning_of_life",
                "target_lag": {"duration": 5, "period": "minutes"},
                "warehouse": "my_warehouse",
            },
            None,
        ),
        (
            {
                "my_name": "my_dynamic_table",
                "schema": {
                    "name": "my_schema",
                    "database": {"name": "my_database"},
                },
                # missing "query"
                "target_lag": {"duration": 5, "period": "minutes"},
                "warehouse": "my_warehouse",
            },
            DbtRuntimeError,
        ),
        (
            {
                "my_name": "my_dynamic_table",
                "schema": {
                    "name": "my_schema",
                    "database": {"name": "my_database"},
                },
                "query": "select 42 from meaning_of_life",
                "target_lag": {"duration": 5, "period": "minutes"},
                # missing "warehouse"
            },
            DbtRuntimeError,
        ),
        (
            {
                "name": "my_indexed_dynamic_table",
                "schema": {
                    "name": "my_schema",
                    "database": {"name": "my_database"},
                },
                "query": "select 42 from meaning_of_life",
                "target_lag": {
                    "duration": -1,
                    "period": "minutes",
                },  # negative `target_lag` is not supported
                "warehouse": "my_warehouse",
            },
            DbtRuntimeError,
        ),
    ],
)
def test_create_dynamic_table(config_dict: dict, exception: Type[Exception]):
    if exception:
        with pytest.raises(exception):
            SnowflakeDynamicTableRelation.from_dict(config_dict)
    else:
        my_dynamic_table = SnowflakeDynamicTableRelation.from_dict(config_dict)

        assert my_dynamic_table.name == config_dict.get("name")
        assert my_dynamic_table.schema_name == config_dict.get("schema", {}).get("name")
        assert my_dynamic_table.database_name == config_dict.get("schema", {}).get(
            "database", {}
        ).get("name")
        assert my_dynamic_table.query == config_dict.get("query")
        assert my_dynamic_table.warehouse == config_dict.get("warehouse")

        default_target_lag = SnowflakeDynamicTableTargetLagRelation.from_dict(
            {"duration": 1, "period": "minutes"}
        )
        default_target_lag_duration = default_target_lag.duration
        default_target_lag_period = default_target_lag.period
        assert my_dynamic_table.target_lag.duration == config_dict.get("target_lag", {}).get(
            "duration", default_target_lag_duration
        )
        assert my_dynamic_table.target_lag.period == config_dict.get("target_lag", {}).get(
            "period", default_target_lag_period
        )


@pytest.mark.parametrize(
    "changes,is_empty,requires_full_refresh",
    [
        ({"warehouse": "my_other_warehouse"}, False, False),
        (
            {
                "target_lag": SnowflakeDynamicTableTargetLagRelation.from_dict(
                    {"duration": 42, "period": "days"}
                )
            },
            False,
            False,
        ),
        ({}, True, False),
    ],
)
def test_create_dynamic_table_changeset(
    dynamic_table_relation, changes, is_empty, requires_full_refresh
):
    existing_dynamic_table = replace(dynamic_table_relation)
    target_dynamic_table = replace(existing_dynamic_table, **changes)

    changeset = SnowflakeDynamicTableRelationChangeset.from_relations(
        existing_dynamic_table, target_dynamic_table
    )
    assert changeset.is_empty is is_empty
    assert changeset.requires_full_refresh is requires_full_refresh
