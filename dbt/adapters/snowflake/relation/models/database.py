from dataclasses import dataclass
from typing import Set

from dbt.adapters.relation.models import DatabaseRelation
from dbt.adapters.validation import ValidationMixin, ValidationRule
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.snowflake.relation.models.policy import SnowflakeRenderPolicy


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeDatabaseRelation(DatabaseRelation, ValidationMixin):
    """
    This config follow the specs found here:
    https://docs.snowflake.com/en/sql-reference/sql/create-database

    The following parameters are configurable by dbt:
    - name: name of the database
    """

    # attribution
    name: str

    # configuration
    render = SnowflakeRenderPolicy

    @property
    def validation_rules(self) -> Set[ValidationRule]:
        return {
            ValidationRule(
                validation_check=len(self.name or "") > 0,
                validation_error=DbtRuntimeError(
                    f"dbt-snowflake requires a name for a database, received: {self.name}"
                ),
            )
        }
