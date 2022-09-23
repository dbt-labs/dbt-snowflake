import pytest
from dbt.tests.adapter.utils import test_timestamps

_MODEL_CURRENT_TIMESTAMP = """
SELECT {{current_timestamp()}} as current_timestamp,
       {{current_timestamp_in_utc()}} as current_timestamp_in_utc,
       {{current_timestamp_backcompat()}} as current_timestamp_backcompat
"""

class TestCurrentTimestampSnowflake(test_timestamps.TestCurrentTimestamps):
    @pytest.fixture(scope="class")
    def models(self):
        return {"get_current_timestamp.sql": _MODEL_CURRENT_TIMESTAMP}

    @pytest.fixture(scope="class")
    def expected_schema(self):
        return {
                "CURRENT_TIMESTAMP": "TIMESTAMP_TZ",
                "CURRENT_TIMESTAMP_IN_UTC": "TIMESTAMP_TZ",
                "CURRENT_TIMESTAMP_BACKCOMPAT": "TIMESTAMP_NTZ",
            }
