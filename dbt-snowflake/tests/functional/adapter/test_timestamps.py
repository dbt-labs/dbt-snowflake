import pytest
from dbt.tests.adapter.utils.test_timestamps import BaseCurrentTimestamps

_MODEL_CURRENT_TIMESTAMP = """
SELECT {{current_timestamp()}} as current_timestamp,
       {{current_timestamp_in_utc_backcompat()}} as current_timestamp_in_utc_backcompat,
       {{current_timestamp_backcompat()}} as current_timestamp_backcompat
"""


class TestCurrentTimestampSnowflake(BaseCurrentTimestamps):
    @pytest.fixture(scope="class")
    def models(self):
        return {"get_current_timestamp.sql": _MODEL_CURRENT_TIMESTAMP}

    @pytest.fixture(scope="class")
    def expected_schema(self):
        return {
            "CURRENT_TIMESTAMP": "TIMESTAMP_TZ",
            "CURRENT_TIMESTAMP_IN_UTC_BACKCOMPAT": "TIMESTAMP_NTZ",
            "CURRENT_TIMESTAMP_BACKCOMPAT": "TIMESTAMP_NTZ",
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
                select convert_timezone('UTC', current_timestamp()) as current_timestamp,
                       convert_timezone('UTC', current_timestamp::TIMESTAMP)::TIMESTAMP as current_timestamp_in_utc_backcompat,
                       current_timestamp::TIMESTAMP as current_timestamp_backcompat
                """
