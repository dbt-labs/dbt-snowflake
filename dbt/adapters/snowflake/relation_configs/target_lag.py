from dataclasses import dataclass
from typing import Any, Dict, Optional, Union

import agate
from dbt.adapters.relation_configs import RelationConfigChange
from dbt.contracts.graph.nodes import ModelNode
from dbt.dataclass_schema import StrEnum

from dbt.adapters.snowflake.relation_configs.base import SnowflakeRelationConfigBase


class SnowflakeDynamicTableTargetLagPeriod(StrEnum):
    seconds = "seconds"
    minutes = "minutes"
    hours = "hours"
    days = "days"
    second = "second"
    minute = "minute"
    hour = "hour"
    day = "day"


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableTargetLagConfig(SnowflakeRelationConfigBase):
    """
    This config follow the specs found here:
    https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table

    The following parameters are configurable by dbt:
    - duration: the numeric part of the lag
    - period: the scale part of the lag

    There are currently no non-configurable parameters.
    """

    duration: int
    period: SnowflakeDynamicTableTargetLagPeriod

    def __str__(self) -> str:
        return f"{self.duration} {self.period}"

    @classmethod
    def from_dict(cls, config_dict) -> "SnowflakeDynamicTableTargetLagConfig":
        kwargs_dict: Dict[str, Union[int, SnowflakeDynamicTableTargetLagPeriod]] = {}

        if duration := config_dict.get("duration"):
            kwargs_dict.update({"duration": int(duration)})

        if period := config_dict.get("period"):
            kwargs_dict.update({"period": SnowflakeDynamicTableTargetLagPeriod(period)})

        target_lag: "SnowflakeDynamicTableTargetLagConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return target_lag

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> Dict[str, Any]:
        """
        Translate ModelNode objects from the user-provided config into a standard dictionary.

        Args:
            model_node: the description of the target lag from the user in this format:

                {
                    "target_lag": "int any("seconds", "minutes", "hours", "days")"
                }

        Returns: a standard dictionary describing this `SnowflakeDynamicTableTargetLagConfig` instance
        """
        target_lag: str = model_node.config.extra["target_lag"]
        return cls._parse_target_lag_string(target_lag)

    @classmethod
    def parse_relation_results(cls, relation_results_entry: agate.Row) -> Dict[str, Any]:
        """
        Translate agate objects from the database into a standard dictionary.

        Args:
            relation_results_entry: the description of the target lag from the database in this format:

                agate.Row({
                    "target_lag": "int any("seconds", "minutes", "hours", "days")"
                })

        Returns: a standard dictionary describing this `SnowflakeDynamicTableTargetLagConfig` instance
        """
        target_lag: str = relation_results_entry["target_lag"]
        return cls._parse_target_lag_string(target_lag)

    @staticmethod
    def _parse_target_lag_string(target_lag: str) -> Dict[str, Union[Optional[Union[int, str]]]]:
        try:
            # Snowflake supports strings like `1     \n    minutes` despite the docs not suggesting that
            duration_str, *_, period = target_lag.split(" ")
            duration = int(duration_str)
        except (AttributeError, IndexError):
            duration, period = None, None

        config_dict = {
            "duration": duration,
            "period": period,
        }
        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableTargetLagConfigChange(RelationConfigChange):
    context: Optional[SnowflakeDynamicTableTargetLagConfig] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False
