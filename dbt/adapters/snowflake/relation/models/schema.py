from dataclasses import dataclass
from typing import Set

from dbt.adapters.relation.models import SchemaRelation
from dbt.adapters.validation import ValidationMixin, ValidationRule
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.snowflake.relation.models.database import SnowflakeDatabaseRelation
from dbt.adapters.snowflake.relation.models.policy import SnowflakeRenderPolicy


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeSchemaRelation(SchemaRelation, ValidationMixin):
    """
    This config follow the specs found here:
    https://docs.snowflake.com/en/sql-reference/sql/create-schema

    The following parameters are configurable by dbt:
    - name: name of the schema
    - database_name: name of the database
    """

    # attribution
    name: str

    # configuration
    render = SnowflakeRenderPolicy
    DatabaseParser = SnowflakeDatabaseRelation  # type: ignore

    @property
    def validation_rules(self) -> Set[ValidationRule]:
        return {
            ValidationRule(
                validation_check=len(self.name or "") > 0,
                validation_error=DbtRuntimeError(
                    f"dbt-snowflake requires a name for a schema, received: {self.name}"
                ),
            )
        }

    @classmethod
    def from_dict(cls, config_dict) -> "SnowflakeSchemaRelation":
        schema = super().from_dict(config_dict)
        assert isinstance(schema, SnowflakeSchemaRelation)
        return schema
