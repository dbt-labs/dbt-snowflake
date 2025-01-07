from unittest import mock

import dbt.adapters.snowflake.__version__

from dbt.adapters.snowflake.impl import SnowflakeAdapter
from dbt.adapters.base.relation import AdapterTrackingRelationInfo


def test_telemetry_with_snowflake_details():
    mock_model_config = mock.MagicMock()
    mock_model_config._extra = mock.MagicMock()
    mock_model_config._extra = {
        "adapter_type": "snowflake",
        "table_format": "iceberg",
    }

    res = SnowflakeAdapter.get_adapter_run_info(mock_model_config)

    assert res.adapter_name == "snowflake"
    assert res.base_adapter_version == dbt.adapters.__about__.version
    assert res.adapter_version == dbt.adapters.snowflake.__version__.version
    assert res.model_adapter_details == {
        "adapter_type": "snowflake",
        "table_format": "iceberg",
    }

    assert type(res) is AdapterTrackingRelationInfo
