from dataclasses import dataclass
from typing import Optional

import agate
from dbt.adapters.relation_configs import RelationConfigChange, RelationResults
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName

from dbt.adapters.snowflake.relation_configs.base import SnowflakeRelationConfigBase
from dbt.adapters.snowflake.relation_configs.target_lag import (
    SnowflakeDynamicTableTargetLagConfig,
    SnowflakeDynamicTableTargetLagConfigChange,
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableConfig(SnowflakeRelationConfigBase):
    """
    This config follow the specs found here:
    https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table

    The following parameters are configurable by dbt:
    - name: name of the dynamic table
    - query: the query behind the table
    - target_lag: the maximum amount of time that the dynamic table’s content should lag behind updates to the base tables
    - warehouse: the name of the warehouse that provides the compute resources for refreshing the dynamic table

    There are currently no non-configurable parameters.
    """

    name: str
    schema_name: str
    database_name: str
    query: str
    target_lag: SnowflakeDynamicTableTargetLagConfig
    warehouse: str

    @classmethod
    def from_dict(cls, config_dict) -> "SnowflakeDynamicTableConfig":
        kwargs_dict = {
            "name": cls._render_part(ComponentName.Identifier, config_dict.get("name")),
            "schema_name": cls._render_part(ComponentName.Schema, config_dict.get("schema_name")),
            "database_name": cls._render_part(
                ComponentName.Database, config_dict.get("database_name")
            ),
            "query": config_dict.get("query"),
            "warehouse": config_dict.get("warehouse"),
        }

        if target_lag := config_dict.get("target_lag"):
            kwargs_dict.update(
                {"target_lag": SnowflakeDynamicTableTargetLagConfig.from_dict(target_lag)}
            )

        dynamic_table: "SnowflakeDynamicTableConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return dynamic_table

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        config_dict = {
            "name": model_node.identifier,
            "schema_name": model_node.schema,
            "database_name": model_node.database,
            "query": model_node.compiled_code,
            "warehouse": model_node.config.extra.get("snowflake_warehouse"),
        }

        if model_node.config.extra.get("target_lag"):
            config_dict.update(
                {"target_lag": SnowflakeDynamicTableTargetLagConfig.parse_model_node(model_node)}
            )

        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> dict:
        dynamic_table: agate.Row = relation_results["dynamic_table"].rows[0]

        config_dict = {
            "name": dynamic_table.get("name"),
            "schema_name": dynamic_table.get("schema_name"),
            "database_name": dynamic_table.get("database_name"),
            "query": dynamic_table.get("text"),
            "warehouse": dynamic_table.get("warehouse"),
        }

        if dynamic_table.get("target_lag"):
            config_dict.update(
                {
                    "target_lag": SnowflakeDynamicTableTargetLagConfig.parse_relation_results(
                        dynamic_table
                    )
                }
            )

        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableWarehouseConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False


@dataclass
class SnowflakeDynamicTableConfigChangeset:
    target_lag: Optional[SnowflakeDynamicTableTargetLagConfigChange] = None
    warehouse: Optional[SnowflakeDynamicTableWarehouseConfigChange] = None

    @property
    def requires_full_refresh(self) -> bool:
        return any(
            [
                self.target_lag.requires_full_refresh if self.target_lag else False,
                self.warehouse.requires_full_refresh if self.warehouse else False,
            ]
        )

    @property
    def has_changes(self) -> bool:
        return any([self.target_lag, self.warehouse])
