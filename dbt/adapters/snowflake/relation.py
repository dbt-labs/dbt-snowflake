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
    def from_describe_relation_results(
        cls, describe_relation_results: RelationResults, relation_type: SnowflakeRelationType
    ) -> RelationConfigBase:
        """
        Produce a validated relation config from a series of "describe <relation>"-type queries.

        The intention is to remove validation from the jinja context and put it in python. This method gets
        called in a jinja template and it's results are used in the jinja template. For an example, please
        refer to `dbt/include/snowflake/macros/materializations/dynamic_table/materialization.sql`.

        * Note: This method was written purposely vague as the intention is to migrate this to dbt-core

        Args:
            describe_relation_results: the results of one or more queries run against the database to describe this relation
            relation_type: the type of relation associated with the relation results

        Returns: a validated Snowflake-specific RelationConfigBase instance
        """
        if relation_config := cls.relation_configs.get(relation_type):
            return relation_config.from_describe_relation_results(describe_relation_results)

        raise DbtRuntimeError(
            f"from_relation_results() is not supported for the provided relation type: {relation_type}"
        )

    @classmethod
    def dynamic_table_config_changeset(
        cls,
        new_dynamic_table: SnowflakeDynamicTableConfig,
        existing_dynamic_table_describe_relation_results: RelationResults,
    ) -> Optional[SnowflakeDynamicTableConfigChangeset]:
        """
        Determine the changes required to update `existing_dynamic_table_relation_results` to `new_dynamic_table`

        `new_dynamic_table` and `existing_dynamic_table_relation_results` effectively describe the same
        dynamic table at two points in time: after this run of dbt (if successful) and current state, respectively.
        This function is used to determine the changes that would need to be applied, if any, so that the
        materialization can determine what course of action to take (full refresh, apply changes, refresh data).

        Args:
            new_dynamic_table: reflects the user's configuration as specified in the model, schema, profile, etc.;
                it's built from the RuntimeConfigObject that's in the jinja context, likely with
                'cls.from_runtime_config()`
            existing_dynamic_table_describe_relation_results: reflects the configuration of the object that currently
                exists in the database; it's built from the macro `snowflake__describe_dynamic_table()`, likely
                with `cls.from_relation_results()`

        Returns: a changeset if there are any changes, otherwise None
        """
        existing_dynamic_table = cls.from_describe_relation_results(
            existing_dynamic_table_describe_relation_results, SnowflakeRelationType.DynamicTable
        )

        try:
            assert isinstance(new_dynamic_table, SnowflakeDynamicTableConfig)
            assert isinstance(existing_dynamic_table, SnowflakeDynamicTableConfig)
        except AssertionError:
            raise DbtRuntimeError(
                f"Two dynamic table configs were expected, but received:"
                f"/n    {new_dynamic_table}"
                f"/n    {existing_dynamic_table}"
            )

        config_changeset = SnowflakeDynamicTableConfigChangeset()

        if new_dynamic_table.target_lag != existing_dynamic_table.target_lag:
            config_changeset.target_lag = SnowflakeDynamicTableTargetLagConfigChange(
                action=RelationConfigChangeAction.alter,
                context=new_dynamic_table.target_lag,
            )

        if new_dynamic_table.warehouse != existing_dynamic_table.warehouse:
            config_changeset.warehouse = SnowflakeDynamicTableWarehouseConfigChange(
                action=RelationConfigChangeAction.alter,
                context=new_dynamic_table.warehouse,
            )

        return config_changeset if config_changeset.has_changes else None
