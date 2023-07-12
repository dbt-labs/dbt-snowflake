from dataclasses import dataclass

from dbt.adapters.relation.models import IncludePolicy, QuotePolicy, RenderPolicy
from dbt.dataclass_schema import StrEnum


class SnowflakeRelationType(StrEnum):
    Table = "table"
    View = "view"
    CTE = "cte"
    External = "external"
    DynamicTable = "dynamic_table"


class SnowflakeIncludePolicy(IncludePolicy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class SnowflakeQuotePolicy(QuotePolicy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


SnowflakeRenderPolicy = RenderPolicy(
    quote_policy=SnowflakeQuotePolicy(),
    include_policy=SnowflakeIncludePolicy(),
    quote_character='"',
    delimiter=".",
)
