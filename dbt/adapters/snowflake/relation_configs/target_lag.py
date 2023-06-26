from dataclasses import dataclass
from typing import Set, Dict, Union

import agate
from dbt.adapters.relation_configs import (
    RelationConfigBase,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
    RelationConfigChange,
    RelationConfigChangeAction,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.dataclass_schema import StrEnum
from dbt.exceptions import DbtRuntimeError


class SnowflakeDynamicTableTargetLagPeriod(StrEnum):
    seconds = "seconds"
    minutes = "minutes"
    minute = "minute"
    hours = "hours"
    hour = "hour"
    days = "days"
    day = "day"


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableTargetLagConfig(RelationConfigBase, RelationConfigValidationMixin):
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

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return {
            RelationConfigValidationRule(
                validation_check=self.duration > 0
                and self.period in SnowflakeDynamicTableTargetLagPeriod,
                validation_error=DbtRuntimeError(
                    f"Target lag for a materialized view requires both a positive duration and a period. Received:\n"
                    f"    duration: {self.duration}"
                    f"    period: {self.period}"
                ),
            ),
            RelationConfigValidationRule(
                validation_check=(
                    self.duration >= 60
                    if self.period == SnowflakeDynamicTableTargetLagPeriod.seconds
                    else self.duration >= 1
                ),
                validation_error=DbtRuntimeError(
                    f"The minimum for target lag for a materialized view is 60 seconds. Received:\n"
                    f"    duration: {self.duration}"
                    f"    period: {self.period}"
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict: dict) -> "SnowflakeDynamicTableTargetLagConfig":
        kwargs_dict: Dict[str, Union[int, SnowflakeDynamicTableTargetLagPeriod]] = {}

        if duration := config_dict.get("duration"):
            kwargs_dict.update({"duration": int(duration)})

        if period := config_dict.get("period"):
            kwargs_dict.update({"period": SnowflakeDynamicTableTargetLagPeriod(period)})

        target_lag: "SnowflakeDynamicTableTargetLagConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return target_lag

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        target_lag: str = model_node.config.extra["target_lag"]
        try:
            duration, period = target_lag.split(" ")
        except (AttributeError, IndexError):
            duration, period = None, None

        config_dict = {
            "duration": duration,
            "period": period,
        }
        return config_dict

    @classmethod
    def parse_describe_relation_results(cls, describe_relation_results: agate.Row) -> dict:
        target_lag = describe_relation_results["target_lag"]
        try:
            duration, period = target_lag.split(" ")
        except (AttributeError, IndexError):
            duration, period = None, None

        config_dict = {
            "duration": duration,
            "period": period,
        }
        return config_dict

    def __str__(self) -> str:
        return f"{self.duration} {self.period}"


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableTargetLagConfigChange(
    RelationConfigChange, RelationConfigValidationMixin
):
    context: SnowflakeDynamicTableTargetLagConfig

    @property
    def requires_full_refresh(self) -> bool:
        return False

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return {
            RelationConfigValidationRule(
                validation_check=self.action == RelationConfigChangeAction.alter,
                validation_error=DbtRuntimeError(
                    f"Target lag should only be altered but {self.action} was received."
                ),
            ),
        }
