from dbt_common.record import record_function

from dbt.adapters.record import RecordReplayCursor
from dbt.adapters.snowflake.record.cursor.sfqid import CursorGetSfqidRecord
from dbt.adapters.snowflake.record.cursor.sqlstate import CursorGetSqlStateRecord


class SnowflakeRecordReplayCursor(RecordReplayCursor):
    """A custom extension of RecordReplayCursor that adds the sqlstate
    and sfqid properties which are specific to snowflake-connector."""

    @property
    @property
    @record_function(CursorGetSqlStateRecord, method=True, id_field_name="connection_name")
    def sqlstate(self):
        return self.native_cursor.sqlstate

    @property
    @record_function(CursorGetSfqidRecord, method=True, id_field_name="connection_name")
    def sfqid(self):
        return self.native_cursor.sfqid
