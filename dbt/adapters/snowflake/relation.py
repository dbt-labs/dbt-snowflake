from dataclasses import dataclass, field
from typing import Optional, Type

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.relation_configs import RelationConfigChangeAction, RelationResults
from dbt.context.providers import RuntimeConfigObject
from dbt.utils import classproperty

from dbt.adapters.snowflake.relation_configs import (
    SnowflakeDynamicTableConfig,
    SnowflakeDynamicTableConfigChangeset,
    SnowflakeDynamicTableTargetLagConfigChange,
    SnowflakeDynamicTableWarehouseConfigChange,
    SnowflakeQuotePolicy,
    SnowflakeRelationType,
)


@dataclass(frozen=True, eq=False, repr=False)
class SnowflakeRelation(BaseRelation):
    type: Optional[SnowflakeRelationType] = None  # type: ignore
    quote_policy: SnowflakeQuotePolicy = field(default_factory=lambda: SnowflakeQuotePolicy())
    renameable_relations = frozenset({SnowflakeRelationType.Table, SnowflakeRelationType.View})
    replaceable_relations = frozenset(
        {
            SnowflakeRelationType.DynamicTable,
            SnowflakeRelationType.Table,
            SnowflakeRelationType.View,
        }
    )

    @property
    def is_dynamic_table(self) -> bool:
        return self.type == SnowflakeRelationType.DynamicTable

    @classproperty
    def DynamicTable(cls) -> str:
        return str(SnowflakeRelationType.DynamicTable)

    @classproperty
    def get_relation_type(cls) -> Type[SnowflakeRelationType]:
        return SnowflakeRelationType

    @classmethod
    def dynamic_table_config_changeset(
        cls, relation_results: RelationResults, runtime_config: RuntimeConfigObject
    ) -> Optional[SnowflakeDynamicTableConfigChangeset]:
        existing_dynamic_table = SnowflakeDynamicTableConfig.from_relation_results(
            relation_results
        )
        new_dynamic_table = SnowflakeDynamicTableConfig.from_model_node(runtime_config.model)

        config_change_collection = SnowflakeDynamicTableConfigChangeset()

        if new_dynamic_table.target_lag != existing_dynamic_table.target_lag:
            config_change_collection.target_lag = SnowflakeDynamicTableTargetLagConfigChange(
                action=RelationConfigChangeAction.alter,
                context=new_dynamic_table.target_lag,
            )

        if new_dynamic_table.snowflake_warehouse != existing_dynamic_table.snowflake_warehouse:
            config_change_collection.snowflake_warehouse = (
                SnowflakeDynamicTableWarehouseConfigChange(
                    action=RelationConfigChangeAction.alter,
                    context=new_dynamic_table.snowflake_warehouse,
                )
            )

        if config_change_collection.has_changes:
            return config_change_collection
        return None
