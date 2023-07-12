from dataclasses import dataclass, field
from typing import Optional, Type

from dbt.adapters.base.relation import BaseRelation
from dbt.utils import classproperty

from dbt.adapters.snowflake.relation.models import (
    SnowflakeIncludePolicy,
    SnowflakeQuotePolicy,
    SnowflakeRelationType,
)


@dataclass(frozen=True, eq=False, repr=False)
class SnowflakeRelation(BaseRelation):
    # we need to overwrite the type annotation for `type` so that mashumaro reads the class correctly
    type: Optional[SnowflakeRelationType] = None  # type: ignore

    include_policy: SnowflakeIncludePolicy = field(
        default_factory=lambda: SnowflakeIncludePolicy()
    )
    quote_policy: SnowflakeQuotePolicy = field(default_factory=lambda: SnowflakeQuotePolicy())

    @property
    def is_dynamic_table(self) -> bool:
        return self.type == SnowflakeRelationType.DynamicTable

    @classproperty
    def DynamicTable(cls) -> str:
        return str(SnowflakeRelationType.DynamicTable)

    @classproperty
    def get_relation_type(cls) -> Type[SnowflakeRelationType]:
        return SnowflakeRelationType
