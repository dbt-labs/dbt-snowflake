from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.adapters.contracts.relation import RelationType


def test_renameable_relation():
    relation = BigQueryRelation.create(
        database="my_db",
        schema="my_schema",
        identifier="my_table",
        type=RelationType.Table,
    )
    assert relation.renameable_relations == frozenset(
        {
            RelationType.Table,
        }
    )
