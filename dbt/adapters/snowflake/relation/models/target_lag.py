from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, Set

from dbt.adapters.relation.models import (
    DescribeRelationResults,
    RelationChange,
    RelationChangeAction,
    RelationComponent,
)
from dbt.adapters.validation import ValidationMixin, ValidationRule
from dbt.contracts.graph.nodes import ParsedNode
from dbt.dataclass_schema import StrEnum
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.snowflake.relation.models.policy import SnowflakeRenderPolicy


class SnowflakeDynamicTableTargetLagPeriod(StrEnum):
    second = "second"
    seconds = "seconds"
    minutes = "minutes"
    minute = "minute"
    hours = "hours"
    hour = "hour"
    days = "days"
    day = "day"


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableTargetLagRelation(RelationComponent, ValidationMixin):
    """
    This config follow the specs found here:
    https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table

    The following parameters are configurable by dbt:
    - duration: the numeric part of the lag
    - period: the scale part of the lag

    There are currently no non-configurable parameters.
    """

    # attribution
    duration: int
    period: SnowflakeDynamicTableTargetLagPeriod

    # configuration
    render = SnowflakeRenderPolicy

    def __str__(self) -> str:
        return f"{self.duration} {self.period}"

    @property
    def validation_rules(self) -> Set[ValidationRule]:
        return {
            ValidationRule(
                validation_check=self.duration > 0
                and self.period in SnowflakeDynamicTableTargetLagPeriod,
                validation_error=DbtRuntimeError(
                    f"Target lag for a materialized view requires both a positive duration and a period. Received:\n"
                    f"    duration: {self.duration}"
                    f"    period: {self.period}"
                ),
            ),
            ValidationRule(
                validation_check=(
                    self.duration >= 60
                    if self.period
                    in (
                        SnowflakeDynamicTableTargetLagPeriod.seconds,
                        SnowflakeDynamicTableTargetLagPeriod.second,
                    )
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
    def from_dict(cls, config_dict: Dict[str, Any]) -> "SnowflakeDynamicTableTargetLagRelation":
        kwargs_dict = deepcopy(config_dict)

        if duration := config_dict.get("duration"):
            kwargs_dict.update({"duration": int(duration)})

        if period := config_dict.get("period"):
            kwargs_dict.update({"period": SnowflakeDynamicTableTargetLagPeriod(period)})

        target_lag = super().from_dict(kwargs_dict)
        assert isinstance(target_lag, SnowflakeDynamicTableTargetLagRelation)
        return target_lag

    @classmethod
    def parse_node(cls, node: ParsedNode) -> Dict[str, Any]:
        target_lag: str = node.config.extra["target_lag"]
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
    def parse_describe_relation_results(
        cls, describe_relation_results: DescribeRelationResults
    ) -> Dict[str, Any]:
        target_lag_entry = cls._parse_single_record_from_describe_relation_results(
            describe_relation_results, "target_lag"
        )
        target_lag: str = target_lag_entry["target_lag"]
        try:
            duration, period = (part for part in target_lag.split(" ") if part.strip() != "")
        except (AttributeError, IndexError):
            duration, period = None, None

        config_dict = {
            "duration": duration,
            "period": period,
        }
        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableTargetLagRelationChange(RelationChange, ValidationMixin):
    context: SnowflakeDynamicTableTargetLagRelation

    @property
    def requires_full_refresh(self) -> bool:
        return False

    @property
    def validation_rules(self) -> Set[ValidationRule]:
        return {
            ValidationRule(
                validation_check=self.action == RelationChangeAction.alter,
                validation_error=DbtRuntimeError(
                    f"Target lag should only be altered but {self.action} was received."
                ),
            ),
        }
