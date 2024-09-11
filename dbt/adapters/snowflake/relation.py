import textwrap

from dataclasses import dataclass, field
from typing import FrozenSet, Optional, Type


from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import ComponentName, RelationConfig
from dbt.adapters.events.types import AdapterEventWarning
from dbt.adapters.relation_configs import (
    RelationConfigBase,
    RelationConfigChangeAction,
    RelationResults,
)
from dbt.adapters.utils import classproperty
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.events.functions import warn_or_error

from dbt.adapters.snowflake.relation_configs import (
    SnowflakeDynamicTableConfig,
    SnowflakeDynamicTableConfigChangeset,
    SnowflakeDynamicTableRefreshModeConfigChange,
    SnowflakeDynamicTableTargetLagConfigChange,
    SnowflakeDynamicTableWarehouseConfigChange,
    SnowflakeObjectFormat,
    SnowflakeQuotePolicy,
    SnowflakeRelationType,
)


@dataclass(frozen=True, eq=False, repr=False)
class SnowflakeRelation(BaseRelation):
    type: Optional[SnowflakeRelationType] = None
    table_format: str = SnowflakeObjectFormat.DEFAULT
    quote_policy: SnowflakeQuotePolicy = field(default_factory=lambda: SnowflakeQuotePolicy())
    require_alias: bool = False
    relation_configs = {
        SnowflakeRelationType.DynamicTable: SnowflakeDynamicTableConfig,
    }
    renameable_relations: FrozenSet[SnowflakeRelationType] = field(
        default_factory=lambda: frozenset(
            {
                SnowflakeRelationType.Table,  # type: ignore
                SnowflakeRelationType.View,  # type: ignore
            }
        )
    )

    replaceable_relations: FrozenSet[SnowflakeRelationType] = field(
        default_factory=lambda: frozenset(
            {
                SnowflakeRelationType.DynamicTable,  # type: ignore
                SnowflakeRelationType.Table,  # type: ignore
                SnowflakeRelationType.View,  # type: ignore
            }
        )
    )

    @property
    def is_dynamic_table(self) -> bool:
        return self.type == SnowflakeRelationType.DynamicTable

    @property
    def is_iceberg_format(self) -> bool:
        return self.table_format == SnowflakeObjectFormat.ICEBERG

    @classproperty
    def DynamicTable(cls) -> str:
        return str(SnowflakeRelationType.DynamicTable)

    @classproperty
    def get_relation_type(cls) -> Type[SnowflakeRelationType]:
        return SnowflakeRelationType

    @classmethod
    def from_config(cls, config: RelationConfig) -> RelationConfigBase:
        relation_type: str = config.config.materialized

        if relation_config := cls.relation_configs.get(relation_type):
            return relation_config.from_relation_config(config)

        raise DbtRuntimeError(
            f"from_config() is not supported for the provided relation type: {relation_type}"
        )

    @classmethod
    def dynamic_table_config_changeset(
        cls, relation_results: RelationResults, relation_config: RelationConfig
    ) -> Optional[SnowflakeDynamicTableConfigChangeset]:
        existing_dynamic_table = SnowflakeDynamicTableConfig.from_relation_results(
            relation_results
        )
        new_dynamic_table = SnowflakeDynamicTableConfig.from_relation_config(relation_config)

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

        if new_dynamic_table.refresh_mode != existing_dynamic_table.refresh_mode:
            config_change_collection.refresh_mode = SnowflakeDynamicTableRefreshModeConfigChange(
                action=RelationConfigChangeAction.create,
                context=new_dynamic_table.refresh_mode,
            )

        if config_change_collection.has_changes:
            return config_change_collection
        return None

    def as_case_sensitive(self) -> "SnowflakeRelation":
        path_part_map = {}

        for path in ComponentName:
            if self.include_policy.get_part(path):
                part = self.path.get_part(path)
                if part:
                    if self.quote_policy.get_part(path):
                        path_part_map[path] = part
                    else:
                        path_part_map[path] = part.upper()

        return self.replace_path(**path_part_map)

    def get_ddl_prefix_for_create(self, config: RelationConfig, temporary: bool):
        """
        This macro renders the appropriate DDL prefix during the create_table_as
        macro. It decides based on mutually exclusive table configuration options:

        - TEMPORARY: Indicates a table that exists only for the duration of the session.
        - ICEBERG: A specific storage format that requires a distinct DDL layout.
        - TRANSIENT: A table similar to a permanent table but without fail-safe.

        Additional Caveats for Iceberg models:
        - transient=true throws a warning because Iceberg does not support transient tables
        - A temporary relation is never an Iceberg relation because Iceberg does not
          support temporary relations.
        """

        transient_explicitly_set_true: bool = config.get("transient", False)

        # Temporary tables are a Snowflake feature that do not exist in the
        # Iceberg framework. We ignore the Iceberg status of the model.
        if temporary:
            return "temporary"
        elif self.is_iceberg_format:
            # Log a warning that transient=true on an Iceberg relation is ignored.
            if transient_explicitly_set_true:
                warn_or_error(
                    AdapterEventWarning(
                        base_msg=(
                            "Iceberg format relations cannot be transient. Please "
                            "remove either the transient or iceberg config options "
                            f"from {self.path.database}.{self.path.schema}."
                            f"{self.path.identifier}. If left unmodified, dbt will "
                            "ignore 'transient'."
                        )
                    )
                )

            return "iceberg"

        # Always supply transient on table create DDL unless user specifically sets
        # transient to false or unset. Might as well update the object attribute too!
        elif transient_explicitly_set_true or config.get("transient") is None:
            return "transient"
        else:
            return ""

    def get_ddl_prefix_for_alter(self):
        """All ALTER statements on Iceberg tables require an ICEBERG prefix"""
        if self.is_iceberg_format:
            return "iceberg"
        else:
            return ""

    def render_iceberg_ddl(self, config: RelationConfig):
        base_location: str = f"_dbt/{self.schema}/{self.name}"

        if subpath := config.get("base_location_subpath"):
            base_location += f"/{subpath}"

        iceberg_ddl_predicates: str = f"""
        external_volume = '{config.get('external_volume')}'
        catalog = 'snowflake'
        base_location = '{base_location}'
        """
        return textwrap.indent(textwrap.dedent(iceberg_ddl_predicates), " " * 10)
