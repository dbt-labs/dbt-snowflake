from dataclasses import dataclass
from typing import Any, Dict, Optional

import agate
from dbt.adapters.relation_configs import RelationConfigChange, RelationResults
from dbt.contracts.graph.nodes import ParsedNode
from dbt.contracts.relation import ComponentName

from dbt.adapters.snowflake.relation_configs.base import SnowflakeRelationConfigBase


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableConfig(SnowflakeRelationConfigBase):
    """
    This config follow the specs found here:
    https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table

    The following parameters are configurable by dbt:
    - name: name of the dynamic table
    - target_lag: the maximum amount of time that the dynamic table’s content should lag behind updates to the base tables
    - snowflake_warehouse: the name of the warehouse that provides the compute resources for refreshing the dynamic table

    There are currently no non-configurable parameters.
    """

    name: str
    schema_name: str
    database_name: str
    target_lag: str
    snowflake_warehouse: str

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "SnowflakeDynamicTableConfig":
        kwargs_dict = {
            "name": cls._render_part(ComponentName.Identifier, config_dict.get("name")),
            "schema_name": cls._render_part(ComponentName.Schema, config_dict.get("schema_name")),
            "database_name": cls._render_part(
                ComponentName.Database, config_dict.get("database_name")
            ),
            "target_lag": config_dict.get("target_lag"),
            "snowflake_warehouse": config_dict.get("snowflake_warehouse"),
        }

        dynamic_table: "SnowflakeDynamicTableConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return dynamic_table

    @classmethod
    def parse_node(cls, node: ParsedNode) -> Dict[str, Any]:
        config_dict = {
            "name": node.identifier,
            "schema_name": node.schema,
            "database_name": node.database,
            "target_lag": node.config.extra.get("target_lag"),
            "snowflake_warehouse": node.config.extra.get("snowflake_warehouse"),
        }

        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> Dict[str, Any]:
        dynamic_table: agate.Row = relation_results["dynamic_table"].rows[0]

        config_dict = {
            "name": dynamic_table.get("name"),
            "schema_name": dynamic_table.get("schema_name"),
            "database_name": dynamic_table.get("database_name"),
            "target_lag": dynamic_table.get("target_lag"),
            "snowflake_warehouse": dynamic_table.get("warehouse"),
        }

        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableTargetLagConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableWarehouseConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False


@dataclass
class SnowflakeDynamicTableConfigChangeset:
    target_lag: Optional[SnowflakeDynamicTableTargetLagConfigChange] = None
    snowflake_warehouse: Optional[SnowflakeDynamicTableWarehouseConfigChange] = None

    @property
    def requires_full_refresh(self) -> bool:
        return any(
            [
                self.target_lag.requires_full_refresh if self.target_lag else False,
                self.snowflake_warehouse.requires_full_refresh
                if self.snowflake_warehouse
                else False,
            ]
        )

    @property
    def has_changes(self) -> bool:
        return any([self.target_lag, self.snowflake_warehouse])
