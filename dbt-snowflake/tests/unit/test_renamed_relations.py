from dbt.adapters.snowflake.relation import SnowflakeRelation
from dbt.adapters.snowflake.relation_configs import SnowflakeRelationType


def test_renameable_relation():
    relation = SnowflakeRelation.create(
        database="my_db",
        schema="my_schema",
        identifier="my_table",
        type=SnowflakeRelationType.Table,
    )
    assert relation.renameable_relations == frozenset(
        {
            SnowflakeRelationType.Table,
            SnowflakeRelationType.View,
        }
    )
