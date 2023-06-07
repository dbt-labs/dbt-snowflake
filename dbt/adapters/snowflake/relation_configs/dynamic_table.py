from dataclasses import dataclass

from dbt.adapters.relation_configs import RelationConfigBase

from dbt.adapters.snowflake.relation_configs.lag import SnowflakeDynamicTableLagConfig


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableConfig(RelationConfigBase):
    """
    This config follow the specs found here:
    https://docs.snowflake.com/en/LIMITEDACCESS/create-dynamic-table

    The following parameters are configurable by dbt:
    - name: name of the dynamic table
    - query: the query behind the table
    - lag: the maximum amount of time that the dynamic table’s content should lag behind updates to the base tables
    - warehouse: the name of the warehouse that provides the compute resources for refreshing the dynamic table

    There are currently no non-configurable parameters.
    """

    name: str
    query: str
    lag: SnowflakeDynamicTableLagConfig
    warehouse: str
