from dataclasses import dataclass, field
from typing import Optional

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.adapters.relation_configs import (
    RelationConfigChangeAction,
    RelationResults,
)
from dbt.context.providers import RuntimeConfigObject
from dbt.dataclass_schema import StrEnum
from dbt.utils import classproperty

from dbt.adapters.snowflake.relation_configs import (
    SnowflakeDynamicTableConfig,
    SnowflakeDynamicTableConfigChangeset,
    SnowflakeDynamicTableTargetLagConfigChange,
    SnowflakeDynamicTableWarehouseConfigChange,
)


class SnowflakeRelationType(StrEnum):
    Table = "table"
    View = "view"
    CTE = "cte"
    External = "external"
    DynamicTable = "dynamic_table"


@dataclass
class SnowflakeQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass(frozen=True, eq=False, repr=False)
class SnowflakeRelation(BaseRelation):
    type: Optional[SnowflakeRelationType] = None  # type: ignore
    quote_policy: SnowflakeQuotePolicy = field(default_factory=lambda: SnowflakeQuotePolicy())

    @property
    def is_dynamic_table(self) -> bool:
        return self.type == SnowflakeRelationType.DynamicTable

    @classproperty
    def DynamicTable(cls) -> str:
        return str(SnowflakeRelationType.DynamicTable)

    def dynamic_table_from_runtime_config(
        self, runtime_config: RuntimeConfigObject
    ) -> SnowflakeDynamicTableConfig:
        dynamic_table = SnowflakeDynamicTableConfig.from_model_node(runtime_config.model)
        return dynamic_table

    def dynamic_table_config_changeset(  # type: ignore
        self,
        dynamic_table: SnowflakeDynamicTableConfig,
        relation_results: RelationResults,
    ) -> Optional[SnowflakeDynamicTableConfigChangeset]:
        config_changeset = SnowflakeDynamicTableConfigChangeset()

        existing_dynamic_table = SnowflakeDynamicTableConfig.from_relation_results(
            relation_results
        )

        if dynamic_table.target_lag != existing_dynamic_table.target_lag:
            config_changeset.target_lag = SnowflakeDynamicTableTargetLagConfigChange(
                action=RelationConfigChangeAction.alter,
                context=dynamic_table.target_lag,
            )

        if dynamic_table.warehouse != existing_dynamic_table.warehouse:
            config_changeset.warehouse = SnowflakeDynamicTableWarehouseConfigChange(
                action=RelationConfigChangeAction.alter,
                context=dynamic_table.warehouse,
            )

        if config_changeset.has_changes:
            return config_changeset
        return None
