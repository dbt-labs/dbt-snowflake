from dbt.adapters.record import RecordReplayHandle

from dbt.adapters.snowflake.record.cursor.cursor import SnowflakeRecordReplayCursor


class SnowflakeRecordReplayHandle(RecordReplayHandle):
    """A custom extension of RecordReplayHandle that returns a
    snowflake-connector-specific SnowflakeRecordReplayCursor object."""

    def cursor(self):
        cursor = None if self.native_handle is None else self.native_handle.cursor()
        return SnowflakeRecordReplayCursor(cursor, self.connection)
