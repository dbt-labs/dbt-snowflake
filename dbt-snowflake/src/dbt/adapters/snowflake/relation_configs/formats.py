from dbt_common.dataclass_schema import StrEnum  # doesn't exist in standard library until py3.11
from typing_extensions import Self


class TableFormat(StrEnum):
    """
    Snowflake docs refers to this an 'Object Format.'
    Data practitioners and interfaces refer to this as 'Table Format's, hence the term's use here.
    """

    DEFAULT = "default"
    ICEBERG = "iceberg"

    @classmethod
    def default(cls) -> Self:
        return cls("default")

    def __str__(self):
        return self.value
