from dbt.adapters.snowflake.materialization import SnowflakeMaterializationType
from dbt.adapters.snowflake.relation.models import (
    SnowflakeDynamicTableTargetLagRelation,
    SnowflakeRelationType,
    SnowflakeDynamicTableTargetLagPeriod,
    SnowflakeRenderPolicy,
)


def test_make_from_node(materialization_factory, dynamic_table_compiled_node):
    materialization = materialization_factory.make_from_node(
        node=dynamic_table_compiled_node,
        existing_relation_ref=None,
    )
    assert materialization.type == SnowflakeMaterializationType.DynamicTable

    dynamic_table = materialization.target_relation
    assert dynamic_table.type == SnowflakeRelationType.DynamicTable

    assert dynamic_table.name == "my_dynamic_table"
    assert dynamic_table.schema_name == "my_schema"
    assert dynamic_table.database_name == "my_database"
    assert dynamic_table.query == "select 42 from meaning_of_life"
    assert dynamic_table.warehouse == "my_warehouse"
    target_lag = SnowflakeDynamicTableTargetLagRelation(
        duration=4,
        period=SnowflakeDynamicTableTargetLagPeriod.minutes,
        render=SnowflakeRenderPolicy,
    )
    assert dynamic_table.target_lag == target_lag
