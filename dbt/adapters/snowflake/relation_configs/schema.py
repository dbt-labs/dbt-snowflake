from dataclasses import dataclass
from typing import Set

from dbt.adapters.relation_configs import (
    RelationResults,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.snowflake.relation_configs.base import SnowflakeRelationConfigBase
from dbt.adapters.snowflake.relation_configs.database import SnowflakeDatabaseConfig


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeSchemaConfig(SnowflakeRelationConfigBase, RelationConfigValidationMixin):
    """
    This config follow the specs found here:
    https://docs.snowflake.com/en/sql-reference/sql/create-schema

    The following parameters are configurable by dbt:
    - name: name of the schema
    - database_name: name of the database
    """

    name: str
    database: SnowflakeDatabaseConfig

    @property
    def database_name(self) -> str:
        return self.database.name

    @property
    def fully_qualified_path(self) -> str:
        return ".".join(
            part for part in [self.database.fully_qualified_path, self.name] if part is not None
        )

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return {
            RelationConfigValidationRule(
                validation_check=all({self.database_name, self.name}),
                validation_error=DbtRuntimeError(
                    f"dbt-snowflake requires a name for a schema, received: {self.name}"
                ),
            )
        }

    @classmethod
    def from_dict(cls, config_dict: dict) -> "SnowflakeSchemaConfig":
        kwargs_dict = {
            "name": cls._render_part(ComponentName.Schema, config_dict.get("name", "")),
            "database": SnowflakeDatabaseConfig.from_dict(config_dict.get("database", {})),
        }

        schema: "SnowflakeSchemaConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return schema

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        config_dict = {
            "name": model_node.schema,
            "database": SnowflakeDatabaseConfig.parse_model_node(model_node),
        }
        return config_dict

    @classmethod
    def parse_describe_relation_results(cls, describe_relation_results: RelationResults) -> dict:
        config_dict = {
            "name": describe_relation_results.get("schema_name"),
            "database": SnowflakeDatabaseConfig.parse_describe_relation_results(
                describe_relation_results
            ),
        }
        return config_dict
