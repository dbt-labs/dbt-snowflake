from dataclasses import dataclass

from dbt.dataclass_schema import StrEnum
from dbt.contracts.graph.model_config import Metadata
from dbt.exceptions import DbtConfigError


class DynamicTableLagPeriod(StrEnum):
    seconds = "seconds"
    minutes = "minutes"
    hours = "hours"
    days = "days"


@dataclass
class DynamicTableLag:
    duration: int
    period: DynamicTableLagPeriod

    def __init__(self, value: str):
        try:
            duration, period = value.split(" ")
            self.duration = int(duration)
            self.period = DynamicTableLagPeriod(period)
        except ValueError:
            raise DbtConfigError(f"Invalid lag configuration provided: {value}")


class DynamicTableRefreshStrategy(Metadata):
    """
    no_wait	(default): dbt will not wait, it will proceed as soon as the DDL statement is issued
    refresh	(optional): dbt will wait until the DT is in the SUCCEEDED state, polling every few seconds its status

    In order to avoid waiting indefinitely, in `refresh` mode, the polling will stop and the run will fail when:
    - after the LAG period has lapsed, any state other than SCHEDULED, EXECUTING is returned
    - after 2 LAG periods have lapsed, regardless of state
    """

    no_wait = "no_wait"
    refreshed = "refresh"

    @classmethod
    def default_field(cls) -> "DynamicTableRefreshStrategy":
        return cls.no_wait

    @classmethod
    def metadata_key(cls) -> str:
        return "refresh_strategy"
