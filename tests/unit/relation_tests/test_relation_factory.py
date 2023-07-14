"""
Uses the following fixtures in `unit/dbt_snowflake_tests/conftest.py`:
- `relation_factory`
- `dynamic_table_ref`
"""
import pytest
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.snowflake.relation import models


def test_make_ref(dynamic_table_ref):
    assert dynamic_table_ref.name == "my_dynamic_table"
    assert dynamic_table_ref.schema_name == "my_schema"
    assert dynamic_table_ref.database_name == "my_database"
    assert dynamic_table_ref.type == "dynamic_table"
    assert dynamic_table_ref.can_be_renamed is False


def test_make_backup_ref(relation_factory, dynamic_table_ref):
    with pytest.raises(DbtRuntimeError):
        relation_factory.make_backup_ref(dynamic_table_ref)


def test_make_intermediate(relation_factory, dynamic_table_ref):
    with pytest.raises(DbtRuntimeError):
        relation_factory.make_intermediate(dynamic_table_ref)


def test_make_from_describe_relation_results(relation_factory, dynamic_table_relation):
    assert dynamic_table_relation.name == "my_dynamic_table"
    assert dynamic_table_relation.schema_name == "my_schema"
    assert dynamic_table_relation.database_name == "my_database"
    assert dynamic_table_relation.query == "select 4 as id, 2 as other_id from meaning_of_life"
    target_lag = models.SnowflakeDynamicTableTargetLagRelation(
        duration=2,
        period=models.SnowflakeDynamicTableTargetLagPeriod.minutes,
        render=models.SnowflakeRenderPolicy,
    )
    assert dynamic_table_relation.target_lag == target_lag
    assert dynamic_table_relation.warehouse == "my_warehouse"


def test_make_from_model_node(relation_factory, dynamic_table_compiled_node):
    dynamic_table = relation_factory.make_from_node(dynamic_table_compiled_node)

    assert dynamic_table.name == "my_dynamic_table"
    assert dynamic_table.schema_name == "my_schema"
    assert dynamic_table.database_name == "my_database"
    assert dynamic_table.query == "select 42 from meaning_of_life"
    target_lag = models.SnowflakeDynamicTableTargetLagRelation(
        duration=4,
        period=models.SnowflakeDynamicTableTargetLagPeriod.minutes,
        render=models.SnowflakeRenderPolicy,
    )
    assert dynamic_table.target_lag == target_lag
    assert dynamic_table.warehouse == "my_warehouse"
