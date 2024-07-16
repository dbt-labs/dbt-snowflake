from dbt.adapters.snowflake.relation import SnowflakeRelation
from dbt.adapters.snowflake.relation_configs import SnowflakeQuotePolicy


def test_relation_as_case_sensitive_quoting_true():
    relation = SnowflakeRelation.create(
        database="My_Db",
        schema="My_ScHeMa",
        identifier="My_TaBlE",
        quote_policy=SnowflakeQuotePolicy(database=False, schema=True, identifier=False),
    )

    case_sensitive_relation = relation.as_case_sensitive()
    case_sensitive_relation.render_limited()

    assert case_sensitive_relation.database == "MY_DB"
    assert case_sensitive_relation.schema == "My_ScHeMa"
    assert case_sensitive_relation.identifier == "MY_TABLE"
    assert case_sensitive_relation.render() == 'MY_DB."My_ScHeMa".MY_TABLE'
