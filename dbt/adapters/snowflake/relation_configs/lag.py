from dataclasses import dataclass

from dbt.adapters.relation_configs import RelationConfigBase
from dbt.dataclass_schema import StrEnum


class SnowflakeDynamicTableLagPeriod(StrEnum):
    seconds = "seconds"
    minutes = "minutes"
    hours = "hours"
    days = "days"


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableLagConfig(RelationConfigBase):
    """
    This config follow the specs found here:
    https://docs.snowflake.com/en/LIMITEDACCESS/create-dynamic-table

    The following parameters are configurable by dbt:
    - duration: the numeric part of the lag
    - period: the scale part of the lag

    There are currently no non-configurable parameters.
    """

    duration: int
    period: SnowflakeDynamicTableLagPeriod
