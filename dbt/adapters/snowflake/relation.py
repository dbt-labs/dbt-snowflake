from dataclasses import dataclass, field
from typing import Optional, Type

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.relation_configs import (
    RelationConfigBase,
    RelationConfigChangeAction,
    RelationResults,
)
from dbt.context.providers import RuntimeConfigObject
from dbt.contracts.graph.nodes import ModelNode
from dbt.dataclass_schema import StrEnum
from dbt.exceptions import DbtRuntimeError
from dbt.utils import classproperty

from dbt.adapters.snowflake.relation_configs import (
    SnowflakeDynamicTableConfig,
    SnowflakeDynamicTableConfigChangeset,
    SnowflakeDynamicTableTargetLagConfigChange,
    SnowflakeDynamicTableWarehouseConfigChange,
    SnowflakeIncludePolicy,
    SnowflakeQuotePolicy,
)


class SnowflakeRelationType(StrEnum):
    Table = "table"
    View = "view"
    CTE = "cte"
    External = "external"
    DynamicTable = "dynamic_table"


@dataclass(frozen=True, eq=False, repr=False)
class SnowflakeRelation(BaseRelation):
    # we need to overwrite the type annotation for `type` so that mashumaro reads the class correctly
    type: Optional[SnowflakeRelationType] = None  # type: ignore

    include_policy: SnowflakeIncludePolicy = field(
        default_factory=lambda: SnowflakeIncludePolicy()
    )
    quote_policy: SnowflakeQuotePolicy = field(default_factory=lambda: SnowflakeQuotePolicy())
    relation_configs = {
        SnowflakeRelationType.DynamicTable: SnowflakeDynamicTableConfig,
    }

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
    def from_runtime_config(cls, runtime_config: RuntimeConfigObject) -> RelationConfigBase:
        """
        Produce a validated relation config from the config available in the global jinja context.

        The intention is to remove validation from the jinja context and put it in python. This method gets
        called in a jinja template and it's results are used in the jinja template. For an example, please
        refer to `dbt/include/snowflake/macros/materializations/dynamic_table/materialization.sql`. In this file,
        the relation config is retrieved right away, to ensure that the config is validated before any sql
        is executed against the database.

        * Note: This method was written purposely vague as the intention is to migrate this to dbt-core

        Args:
            runtime_config: the `config` RuntimeConfigObject instance that's in the global jinja context

        Returns: a validated Snowflake-specific RelationConfigBase instance
        """
        model_node: ModelNode = runtime_config.model
        relation_type = SnowflakeRelationType(model_node.config.materialized)

        if relation_config := cls.relation_configs.get(relation_type):
            return relation_config.from_model_node(model_node)

        raise DbtRuntimeError(
            f"from_runtime_config() is not supported for the provided relation type: {relation_type}"
        )

    @classmethod
    def dynamic_table_config_changeset(
        cls,
        dynamic_table: SnowflakeDynamicTableConfig,
        relation_results: RelationResults,
    ) -> Optional[SnowflakeDynamicTableConfigChangeset]:
        config_changeset = SnowflakeDynamicTableConfigChangeset()

        existing_dynamic_table = SnowflakeDynamicTableConfig.from_relation_results(
            relation_results
        )
        assert isinstance(existing_dynamic_table, SnowflakeDynamicTableConfig)

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
