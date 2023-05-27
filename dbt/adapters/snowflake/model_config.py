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
    no_wait = "no_wait"
    refreshed = "refresh"

    @classmethod
    def default_field(cls) -> "DynamicTableRefreshStrategy":
        # TODO: which is the default?
        return cls.no_wait

    @classmethod
    def metadata_key(cls) -> str:
        return "refresh_strategy"
