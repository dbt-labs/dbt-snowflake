from dbt.adapters.snowflake.relation_configs.dynamic_table import (  # noqa: F401
    SnowflakeDynamicTableConfig,
    SnowflakeDynamicTableWarehouseConfigChange,
    SnowflakeDynamicTableConfigChangeset,
)
from dbt.adapters.snowflake.relation_configs.target_lag import (  # noqa: F401
    SnowflakeDynamicTableTargetLagConfig,
    SnowflakeDynamicTableTargetLagPeriod,
    SnowflakeDynamicTableTargetLagConfigChange,
)