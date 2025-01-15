from typing import Dict, Optional, Any

import textwrap

from dbt.adapters.base import BaseRelation
from dbt.adapters.contracts.catalog import CatalogIntegration, CatalogIntegrationType
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs import RelationResults


class SnowflakeManagedIcebergCatalogIntegration(CatalogIntegration):
    catalog_type = CatalogIntegrationType.managed

    def render_ddl_predicates(self, relation: BaseRelation, config: RelationConfig) -> str:
        """
        {{ optional('external_volume', dynamic_table.catalog.external_volume) }}
        {{ optional('catalog', dynamic_table.catalog.name) }}
        base_location = '{{ dynamic_table.catalog.base_location }}'
        :param config:
        :param relation:
        :return:
        """
        base_location: str = f"_dbt/{relation.schema}/{relation.name}"

        if sub_path := config.get("base_location_subpath"):
            base_location += f"/{sub_path}"

        iceberg_ddl_predicates: str = f"""
                external_volume = '{self.external_volume}'
                catalog = 'snowflake'
                base_location = '{base_location}'
                """
        return textwrap.indent(textwrap.dedent(iceberg_ddl_predicates), " " * 10)

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> Dict[str, Any]:
        import agate

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


class SnowflakeGlueCatalogIntegration(CatalogIntegration):
    catalog_type = CatalogIntegrationType.glue
    auto_refresh: Optional[str] = None  # "TRUE" | "FALSE"
    replace_invalid_characters: Optional[str] = None  # "TRUE" | "FALSE"

    def _handle_adapter_configs(self, adapter_configs: Optional[Dict]) -> None:
        if adapter_configs:
            if "auto_refresh" in adapter_configs:
                self.auto_refresh = adapter_configs["auto_refresh"]
            if "replace_invalid_characters" in adapter_configs:
                self.replace_invalid_characters = adapter_configs["replace_invalid_characters"]

    def render_ddl_predicates(self, relation: BaseRelation, config: RelationConfig) -> str:
        ddl_predicate = f"""
                   external_volume = '{self.external_volume}'
                   catalog = '{self.integration_name}'
                   """
        if self.namespace:
            ddl_predicate += f"CATALOG_NAMESPACE = '{self.namespace}'\n"
        if self.auto_refresh:
            ddl_predicate += f"auto_refresh = {self.auto_refresh}\n"
        if self.replace_invalid_characters:
            ddl_predicate += f"replace_invalid_characters = {self.replace_invalid_characters}\n"
        return ddl_predicate
