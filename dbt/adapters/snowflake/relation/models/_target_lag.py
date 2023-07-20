from dataclasses import dataclass

from dbt.adapters.relation.models import RelationComponent
from dbt.dataclass_schema import StrEnum


class SnowflakeTargetLagPeriod(StrEnum):
    seconds = "seconds"
    minutes = "minutes"
    hours = "hours"
    days = "days"


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeTargetLagRelation(RelationComponent):
    """
    This config follow the specs found here:
    https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table

    The following parameters are configurable by dbt:
    - duration: the numeric part of the lag
    - period: the scale part of the lag

    There are currently no non-configurable parameters.
    """

    duration: int
    period: SnowflakeTargetLagPeriod
