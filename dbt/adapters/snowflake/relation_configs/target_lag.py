from dataclasses import dataclass

from dbt.adapters.relation_configs import RelationConfigBase
from dbt.dataclass_schema import StrEnum


class SnowflakeDynamicTableTargetLagPeriod(StrEnum):
    seconds = "seconds"
    minutes = "minutes"
    hours = "hours"
    days = "days"


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableTargetLagConfig(RelationConfigBase):
    """
    This config follow the specs found here:
    TODO: add URL once it's GA

    The following parameters are configurable by dbt:
    - duration: the numeric part of the lag
    - period: the scale part of the lag

    There are currently no non-configurable parameters.
    """

    duration: int
    period: SnowflakeDynamicTableTargetLagPeriod
