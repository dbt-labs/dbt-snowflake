from dataclasses import dataclass
from typing import Set, Optional

from dbt.adapters.relation_configs import (
    RelationResults,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
    RelationConfigChange,
    RelationConfigChangeAction,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.snowflake.relation_configs.base import SnowflakeRelationConfigBase
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
    schema_name: str
    database_name: str
    query: str
    target_lag: SnowflakeDynamicTableTargetLagConfig
    warehouse: str

    @property
    def path(self) -> str:
        return ".".join(
            part for part in [self.database_name, self.schema_name, self.name] if part is not None
        )

    @property
    def schema_path(self) -> str:
        return ".".join(
            part for part in [self.database_name, self.schema_name] if part is not None
        )

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return super().validation_rules

    @classmethod
    def from_dict(cls, config_dict: dict) -> "SnowflakeDynamicTableConfig":
        kwargs_dict = {
            "query": config_dict.get("query"),
            "warehouse": config_dict.get("warehouse"),
        }

        if name := config_dict.get("name"):
            kwargs_dict.update({"name": cls._render_part(ComponentName.Identifier, name)})
        if schema_name := config_dict.get("schema_name"):
            kwargs_dict.update(
                {"schema_name": cls._render_part(ComponentName.Schema, schema_name)}
            )
        if database_name := config_dict.get("database_name"):
            kwargs_dict.update(
                {"database_name": cls._render_part(ComponentName.Database, database_name)}
            )

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
            "target_lag": SnowflakeDynamicTableTargetLagConfig.parse_model_node(model_node),
            "warehouse": model_node.config.extra.get("warehouse"),
        }
        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> dict:
        if dynamic_table := relation_results.get("dynamic_table"):
            dynamic_table_config = dynamic_table.rows[0]
        else:
            dynamic_table_config = {}

        config_dict = {
            "name": dynamic_table_config.get("name"),
            "schema_name": dynamic_table_config.get("schema_name"),
            "database_name": dynamic_table_config.get("database_name"),
            "query": cls._parse_query(dynamic_table_config.get("text")),
            "target_lag": SnowflakeDynamicTableTargetLagConfig.parse_relation_results(
                relation_results
            ),
            "warehouse": dynamic_table_config.get("warehouse"),
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

    @property
    def requires_full_refresh(self) -> bool:
        return any(
            {
                self.target_lag.requires_full_refresh if self.target_lag else False,
                self.warehouse.requires_full_refresh if self.warehouse else False,
            }
        )

    @property
    def has_changes(self) -> bool:
        return any({self.target_lag, self.warehouse})
