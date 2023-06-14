from dataclasses import dataclass
from typing import Set, Optional

from dbt.adapters.relation_configs import (
    RelationConfigBase,
    RelationResults,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
    RelationConfigChange,
    RelationConfigChangeAction,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.snowflake.relation_configs.target_lag import (
    SnowflakeDynamicTableTargetLagConfig,
    SnowflakeDynamicTableTargetLagConfigChange,
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDynamicTableConfig(RelationConfigBase, RelationConfigValidationMixin):
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
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return super().validation_rules

    @classmethod
    def from_dict(cls, config_dict: dict) -> "SnowflakeDynamicTableConfig":
        kwargs_dict = {
            "name": config_dict.get("name"),
            "schema_name": config_dict.get("schema_name"),
            "database_name": config_dict.get("database_name"),
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
    def from_model_node(cls, model_node: ModelNode) -> "SnowflakeDynamicTableConfig":
        dynamic_table_config = cls.parse_model_node(model_node)
        dynamic_table = cls.from_dict(dynamic_table_config)
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
    def from_relation_results(
        cls, relation_results: RelationResults
    ) -> "SnowflakeDynamicTableConfig":
        dynamic_table_config = cls.parse_relation_results(relation_results)
        dynamic_table = cls.from_dict(dynamic_table_config)
        return dynamic_table

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
            "query": dynamic_table_config.get("text"),
            "target_lag": SnowflakeDynamicTableTargetLagConfig.parse_relation_results(
                relation_results
            ),
            "warehouse": dynamic_table_config.get("warehouse"),
        }
        return config_dict


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
