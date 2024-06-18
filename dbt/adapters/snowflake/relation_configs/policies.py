from dataclasses import dataclass

from dbt.adapters.base.relation import Policy
from dbt_common.dataclass_schema import StrEnum


class SnowflakeRelationType(StrEnum):
    Table = "table"
    View = "view"
    CTE = "cte"
    External = "external"
    DynamicTable = "dynamic_table"


class SnowflakeIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class SnowflakeQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False
