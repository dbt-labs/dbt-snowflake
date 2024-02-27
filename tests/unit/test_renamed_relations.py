from dbt.adapters.snowflake.relation import SnowflakeRelation
from dbt.adapters.contracts.relation import RelationType


def test_renameable_relation():
    relation = SnowflakeRelation.create(
        database="my_db",
        schema="my_schema",
        identifier="my_table",
        type=RelationType.Table,
    )
    assert relation.renameable_relations == frozenset(
        {
            SnowflakeRelationType.Table,
            SnowflakeRelationType.View,
        }
    )
