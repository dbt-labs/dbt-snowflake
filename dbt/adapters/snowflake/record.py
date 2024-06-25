import dataclasses
from typing import Optional

from dbt.adapters.record import RecordReplayHandle, RecordReplayCursor
from dbt_common.record import record_function, Record, Recorder


class SnowflakeRecordReplayHandle(RecordReplayHandle):
    def cursor(self):
        cursor = None if self.native_handle is None else self.native_handle.cursor()
        return SnowflakeRecordReplayCursor(cursor, self.connection)


@dataclasses.dataclass
class CursorGetSqlStateParams:
    connection_name: str


@dataclasses.dataclass
class CursorGetSqlStateResult:
    msg: Optional[str]


class CursorGetSqlStateRecord(Record):
    params_cls = CursorGetSqlStateParams
    result_cls = CursorGetSqlStateResult


Recorder.register_record_type(CursorGetSqlStateRecord)


@dataclasses.dataclass
class CursorGetSqfidParams:
    connection_name: str


@dataclasses.dataclass
class CursorGetSqfidResult:
    msg: Optional[str]


class CursorGetSqfidRecord(Record):
    params_cls = CursorGetSqfidParams
    result_cls = CursorGetSqfidResult


Recorder.register_record_type(CursorGetSqfidRecord)


class SnowflakeRecordReplayCursor(RecordReplayCursor):
    @property
    @record_function(CursorGetSqlStateRecord, method=True, id_field_name="connection_name")
    def sqlstate(self):
        return self.native_cursor.sqlstate

    @property
    @record_function(CursorGetSqfidRecord, method=True, id_field_name="connection_name")
    def sfqid(self):
        return self.native_cursor.sfqid
