from typing import Dict, Optional

import textwrap

from dbt.adapters.base import BaseRelation
from dbt.adapters.contracts.catalog import CatalogIntegration, CatalogIntegrationType
from dbt.adapters.contracts.relation import RelationConfig


class SnowflakeManagedIcebergCatalogIntegration(CatalogIntegration):
    catalog_type = CatalogIntegrationType.managed

    def render_ddl_predicates(self, relation: RelationConfig) -> str:
        """
        {{ optional('external_volume', dynamic_table.catalog.external_volume) }}
        {{ optional('catalog', dynamic_table.catalog.name) }}
        base_location = '{{ dynamic_table.catalog.base_location }}'
        :param relation:
        :return:
        """
        base_location: str = f"_dbt/{relation.schema}/{relation.name}"

        if sub_path := relation.config.get("base_location_subpath"):
            base_location += f"/{sub_path}"

        iceberg_ddl_predicates: str = f"""
                external_volume = '{self.external_volume}'
                catalog = 'snowflake'
                base_location = '{base_location}'
                """
        return textwrap.indent(textwrap.dedent(iceberg_ddl_predicates), " " * 10)


class SnowflakeGlueCatalogIntegration(CatalogIntegration):
    catalog_type = CatalogIntegrationType.glue
    auto_refresh: str = "FALSE"
    replace_invalid_characters: str = "FALSE"

    def _handle_adapter_configs(self, adapter_configs: Optional[Dict]) -> None:
        if adapter_configs:
            if "auto_refresh" in adapter_configs:
                self.auto_refresh = adapter_configs["auto_refresh"]
            if "replace_invalid_characters" in adapter_configs:
                self.replace_invalid_characters = adapter_configs["replace_invalid_characters"]

    def render_ddl_predicates(self, relation: BaseRelation) -> str:
        ddl_predicate = f"""create or replace iceberg table {relation.render()}
                   external_volume = '{self.external_volume}
                   catalog = '{self.name}'
                   """
        if self.namespace:
            ddl_predicate += "CATALOG_NAMESPACE = '{self.namespace}'"
        if self.auto_refresh:
            ddl_predicate += f"REPLACE_INVALID_CHARACTERS = {self.auto_refresh}"
        if self.replace_invalid_characters:
            ddl_predicate += f"AUTO_REFRESH = {self.replace_invalid_characters}"
        return ddl_predicate
