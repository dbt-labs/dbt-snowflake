from dataclasses import dataclass
from typing import Set

import agate
from dbt.adapters.relation_configs import (
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.snowflake.relation_configs.base import SnowflakeRelationConfigBase


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDatabaseConfig(SnowflakeRelationConfigBase, RelationConfigValidationMixin):
    """
    This config follow the specs found here:
    https://docs.snowflake.com/en/sql-reference/sql/create-database

    The following parameters are configurable by dbt:
    - name: name of the database
    """

    name: str

    @property
    def fully_qualified_path(self) -> str:
        return self.name

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return {
            RelationConfigValidationRule(
                validation_check=len(self.name or "") > 0,
                validation_error=DbtRuntimeError(
                    f"dbt-snowflake requires a name for a database, received: {self.name}"
                ),
            )
        }

    @classmethod
    def from_dict(cls, config_dict: dict) -> "SnowflakeDatabaseConfig":
        kwargs_dict = {
            "name": cls._render_part(ComponentName.Database, config_dict["name"]),
        }

        database: "SnowflakeDatabaseConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return database

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        config_dict = {
            "name": model_node.database,
        }
        return config_dict

    @classmethod
    def parse_describe_relation_results(cls, describe_relation_results: agate.Row) -> dict:
        config_dict = {
            "name": describe_relation_results["database_name"],
        }
        return config_dict
