from dataclasses import dataclass
from typing import Set, Dict, Union

from dbt.adapters.relation_configs import (
    RelationConfigBase,
    RelationResults,
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
    hours = "hours"
    days = "days"


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
                validation_check=self.duration is not None and self.period is not None,
                validation_error=DbtRuntimeError(
                    "Target lag for a materialized view requires both a duration and a period."
                ),
            ),
            RelationConfigValidationRule(
                validation_check=(
                    (
                        self.duration >= 1
                        and self.period != SnowflakeDynamicTableTargetLagPeriod.seconds
                    )
                    or (
                        self.duration >= 60
                        and self.period == SnowflakeDynamicTableTargetLagPeriod.seconds
                    )
                ),
                validation_error=DbtRuntimeError(
                    f"The minimum for target lag for a materialized view is 1 minutes. "
                    f"The provided value is {self.duration} {self.period.value}."
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
        target_lag: str = model_node.config.extra.get("target_lag")
        try:
            duration, period = target_lag.split(" ")
        except IndexError:
            duration, period = None, None

        config_dict = {
            "duration": duration,
            "period": period,
        }
        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> dict:
        if materialized_view := relation_results.get("materialized_view"):
            materialized_view_config = materialized_view.rows[0]
        else:
            materialized_view_config = {}

        target_lag = materialized_view_config.get("target_lag")
        try:
            duration, period = target_lag.split(" ")
        except IndexError:
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
