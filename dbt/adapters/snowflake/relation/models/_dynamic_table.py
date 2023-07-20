from dataclasses import dataclass

from dbt.adapters.relation.models import RelationComponent

from dbt.adapters.snowflake.relation.models._target_lag import SnowflakeTargetLagRelation


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableRelation(RelationComponent):
    """
    This config follow the specs found here:
    https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table

    The following parameters are configurable by dbt:
    - name: name of the dynamic table
    - query: the query behind the table
    - lag: the maximum amount of time that the dynamic table’s content should lag behind updates to the base tables
    - warehouse: the name of the warehouse that provides the compute resources for refreshing the dynamic table

    There are currently no non-configurable parameters.
    """

    name: str
    query: str
    target_lag: SnowflakeTargetLagRelation
    warehouse: str
