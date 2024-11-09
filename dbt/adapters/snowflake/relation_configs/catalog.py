from dataclasses import dataclass
from typing import Any, Dict, Optional, TYPE_CHECKING, Set, List

if TYPE_CHECKING:
    import agate

from dbt.adapters.relation_configs import (
    RelationConfigChange,
    RelationResults,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt.adapters.contracts.relation import RelationConfig
from dbt_common.exceptions import DbtConfigError
from typing_extensions import Self

from dbt.adapters.snowflake.relation_configs.base import SnowflakeRelationConfigBase
from dbt.adapters.snowflake.relation_configs.formats import TableFormat


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeCatalogConfig(SnowflakeRelationConfigBase, RelationConfigValidationMixin):
    """
    This config follow the specs found here:
    https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table
    https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table#create-dynamic-iceberg-table

    The following parameters are configurable by dbt:
    - table_format: format for interfacing with the table, e.g. default, iceberg
    - external_volume: name of the external volume in Snowflake
    - base_location: the directory within the external volume that contains the data
        *Note*: This directory can’t be changed after you create a table.

    The following parameters are not currently configurable by dbt:
    - name: snowflake
    """

    table_format: Optional[TableFormat] = TableFormat.default()
    name: Optional[str] = "SNOWFLAKE"
    external_volume: Optional[str] = None
    base_location: Optional[str] = None

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return {
            RelationConfigValidationRule(
                (self.table_format == "default")
                or (self.table_format == "iceberg" and self.base_location is not None),
                DbtConfigError("Please provide a `base_location` when using iceberg"),
            ),
            RelationConfigValidationRule(
                (self.table_format == "default")
                or (self.table_format == "iceberg" and self.name == "SNOWFLAKE"),
                DbtConfigError(
                    "Only Snowflake catalogs are currently supported when using iceberg"
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> Self:
        kwargs_dict = {
            "name": config_dict.get("name"),
            "external_volume": config_dict.get("external_volume"),
            "base_location": config_dict.get("base_location"),
        }
        if table_format := config_dict.get("table_format"):
            kwargs_dict["table_format"] = TableFormat(table_format)
        return super().from_dict(kwargs_dict)

    @classmethod
    def parse_relation_config(cls, relation_config: RelationConfig) -> Dict[str, Any]:

        if relation_config.config.extra.get("table_format") is None:
            return {}

        config_dict = {
            "table_format": relation_config.config.extra.get("table_format"),
            "name": "SNOWFLAKE",  # this is not currently configurable
        }

        if external_volume := relation_config.config.extra.get("external_volume"):
            config_dict["external_volume"] = external_volume

        catalog_dirs: List[str] = ["_dbt", relation_config.schema, relation_config.name]
        if base_location_subpath := relation_config.config.extra.get("base_location_subpath"):
            catalog_dirs.append(base_location_subpath)
        config_dict["base_location"] = "/".join(catalog_dirs)

        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> Dict[str, Any]:
        # this try block can be removed once enable_iceberg_materializations is retired
        try:
            catalog_results: "agate.Table" = relation_results["catalog"]
        except KeyError:
            # this happens when `enable_iceberg_materializations` is turned off
            return {}

        if len(catalog_results) == 0:
            # this happens when the dynamic table is a standard dynamic table (e.g. not iceberg)
            return {}

        # for now, if we get catalog results, it's because this is an iceberg table
        # this is because we only run `show iceberg tables` to get catalog metadata
        # this will need to be updated once this is in `show objects`
        catalog: "agate.Row" = catalog_results.rows[0]
        config_dict = {
            "table_format": "iceberg",
            "name": catalog.get("catalog_name"),
            "external_volume": catalog.get("external_volume_name"),
            "base_location": catalog.get("base_location"),
        }

        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeCatalogConfigChange(RelationConfigChange):
    context: Optional[SnowflakeCatalogConfig] = None

    @property
    def requires_full_refresh(self) -> bool:
        return True
