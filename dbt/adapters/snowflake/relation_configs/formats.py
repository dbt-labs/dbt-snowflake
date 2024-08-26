from dbt_common.dataclass_schema import StrEnum  # doesn't exist in standard library until py3.11


class SnowflakeObjectFormat(StrEnum):
    DEFAULT = "default"
    ICEBERG = "iceberg"
