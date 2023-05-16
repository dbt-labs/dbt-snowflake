from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.utils import classproperty
from dbt.exceptions import DbtRuntimeError


class SnowflakeRelationType(Enum):
    Table = "table"
    View = "view"
    CTE = "cte"
    External = "external"
    DynamicTable = "dynamic_table"


@dataclass
class SnowflakeQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass(frozen=True, eq=False, repr=False)
class SnowflakeRelation(BaseRelation):
    type: Optional[SnowflakeRelationType] = None  # type: ignore
    quote_policy: SnowflakeQuotePolicy = field(default_factory=lambda: SnowflakeQuotePolicy())

    @property
    def is_dynamic_table(self) -> bool:
        return self.type == SnowflakeRelationType.DynamicTable

    @classproperty
    def DynamicTable(cls) -> str:
        return str(SnowflakeRelationType.DynamicTable)

    @property
    def is_materialized_view(self) -> bool:
        raise DbtRuntimeError("Materialized Views are not supported for dbt-snowflake")

    @classproperty
    def MaterializedView(cls) -> str:
        raise DbtRuntimeError("Materialized Views are not supported for dbt-snowflake")
