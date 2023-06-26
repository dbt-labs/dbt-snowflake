from dataclasses import dataclass
from typing import Set, Dict, Optional

import agate
from dbt.adapters.relation_configs import (
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
    RelationConfigChange,
    RelationConfigChangeAction,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.snowflake.relation_configs.base import SnowflakeRelationConfigBase
from dbt.adapters.snowflake.relation_configs.database import SnowflakeDatabaseConfig
from dbt.adapters.snowflake.relation_configs.schema import SnowflakeSchemaConfig
from dbt.adapters.snowflake.relation_configs.target_lag import (
    SnowflakeDynamicTableTargetLagConfig,
    SnowflakeDynamicTableTargetLagConfigChange,
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableConfig(SnowflakeRelationConfigBase, RelationConfigValidationMixin):
    """
    This config follow the specs found here:
    TODO: add URL once it's GA

    The following parameters are configurable by dbt:
    - name: name of the dynamic table
    - query: the query behind the table
    - lag: the maximum amount of time that the dynamic table’s content should lag behind updates to the base tables
    - warehouse: the name of the warehouse that provides the compute resources for refreshing the dynamic table

    There are currently no non-configurable parameters.
    """

    name: str
    schema: SnowflakeSchemaConfig
    query: str
    target_lag: SnowflakeDynamicTableTargetLagConfig
    warehouse: str

    @property
    def schema_name(self) -> str:
        return self.schema.name

    @property
    def database(self) -> SnowflakeDatabaseConfig:
        return self.schema.database

    @property
    def database_name(self) -> str:
        return self.database.name

    @property
    def fully_qualified_path(self) -> str:
        return ".".join(
            part for part in [self.schema.fully_qualified_path, self.name] if part is not None
        )

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return {
            RelationConfigValidationRule(
                validation_check=len(self.name or "") > 0,
                validation_error=DbtRuntimeError(
                    f"dbt-snowflake requires a name for a dynamic table, received: {self.name}"
                ),
            ),
            RelationConfigValidationRule(
                validation_check=all({self.database_name, self.schema_name, self.name}),
                validation_error=DbtRuntimeError(
                    f"dbt-snowflake requires all three parts of an object's path, received:/n"
                    f"    database: {self.database_name}/n"
                    f"    schema: {self.schema_name}/n"
                    f"    identifier: {self.name}/n"
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict: dict) -> "SnowflakeDynamicTableConfig":
        kwargs_dict = {
            "name": cls._render_part(ComponentName.Identifier, config_dict["name"]),
            "schema": SnowflakeSchemaConfig.from_dict(config_dict["schema"]),
            "query": config_dict["query"],
            "target_lag": SnowflakeDynamicTableTargetLagConfig.from_dict(
                config_dict["target_lag"]
            ),
            "warehouse": config_dict["warehouse"],
        }

        dynamic_table: "SnowflakeDynamicTableConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return dynamic_table

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        config_dict = {
            "name": model_node.identifier,
            "schema": SnowflakeSchemaConfig.parse_model_node(model_node),
            "query": (model_node.compiled_code or "").strip(),
            "target_lag": SnowflakeDynamicTableTargetLagConfig.parse_model_node(model_node),
            "warehouse": model_node.config.extra["warehouse"],
        }
        return config_dict

    @classmethod
    def parse_describe_relation_results(
        cls, describe_relation_results: Dict[str, agate.Table]
    ) -> dict:
        if dynamic_table := describe_relation_results.get("dynamic_table"):
            dynamic_table_config: agate.Row = dynamic_table.rows[0]
        else:
            dynamic_table_config = agate.Row(values={})

        config_dict = {
            "name": dynamic_table_config["name"],
            "schema": SnowflakeSchemaConfig.parse_describe_relation_results(dynamic_table_config),
            "query": cls._parse_query(dynamic_table_config["text"]),
            "target_lag": SnowflakeDynamicTableTargetLagConfig.parse_describe_relation_results(
                dynamic_table_config
            ),
            "warehouse": dynamic_table_config["warehouse"],
        }
        return config_dict

    @classmethod
    def _parse_query(cls, query: str) -> str:
        """
        Get the select statement from the dynamic table definition in Snowflake.

        Args:
            query: the `create dynamic table` statement from `show dynamic tables`, for example:

            create dynamic table my_dynamic_table
                target_lag = '1 minute'
                warehouse = MY_WAREHOUSE
                as (
                    select * from my_base_table
                )
            ;

        Returns: the `select ...` statement, for example:

            select * from my_base_table

        """
        open_paren = query.find("as (") + len("as (")
        close_paren = query.rindex(")")
        return query[open_paren:close_paren].strip()


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableWarehouseConfigChange(
    RelationConfigChange, RelationConfigValidationMixin
):
    context: str

    @property
    def requires_full_refresh(self) -> bool:
        return False

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return {
            RelationConfigValidationRule(
                validation_check=self.action == RelationConfigChangeAction.alter,
                validation_error=DbtRuntimeError(
                    f"Warehouse should only be altered but {self.action} was received."
                ),
            ),
        }


@dataclass
class SnowflakeDynamicTableConfigChangeset:
    target_lag: Optional[SnowflakeDynamicTableTargetLagConfigChange] = None
    warehouse: Optional[SnowflakeDynamicTableWarehouseConfigChange] = None
    _requires_full_refresh_override: Optional[bool] = False

    @property
    def requires_full_refresh(self) -> bool:
        return any(
            {
                self.target_lag.requires_full_refresh if self.target_lag else False,
                self.warehouse.requires_full_refresh if self.warehouse else False,
                self._requires_full_refresh_override,
            }
        )

    @property
    def is_empty(self) -> bool:
        return not any({self.target_lag, self.warehouse, self._requires_full_refresh_override})

    def force_full_refresh(self):
        self._requires_full_refresh_override = True
