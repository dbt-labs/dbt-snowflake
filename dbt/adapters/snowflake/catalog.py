from dbt.adapters.base import BaseRelation
from dbt.adapters.base.catalog import ExternalCatalogIntegration


class SnowflakeExternalCatalogIntegration(ExternalCatalogIntegration):

    def relation_exists(self, relation: BaseRelation) -> bool:
        response, result = self._connection_manager.execute(f"DESCRIBE ICEBERG TABLE {relation.render()}")
        if result and result.rows:
            return True
        return False

    def _exists(self) -> bool:
        if not self._exists:
            response, result = self._connection_manager.execute(
                f"DESCRIBE CATALOG INTEGRATION {self.external_catalog.name}")
            if result and result.rows:
                self._exists = True
            else:
                self._exists = False
        return self._exists

    def refresh_relation(self, relation: BaseRelation) -> None:
        self._connection_manager.execute(f"ALTER ICEBERG TABLE {relation.render()} REFRESH")

    def create_relation(self, relation: BaseRelation) -> None:
        self._connection_manager.execute(f"CREATE ICEBERG TABLE {relation.render()}"
                                         f"EXTERNAL_VOLUME '{self.external_catalog.configuration.external_volume.name}'"
                                         f"CATALOG='{self.external_catalog.name}'")
