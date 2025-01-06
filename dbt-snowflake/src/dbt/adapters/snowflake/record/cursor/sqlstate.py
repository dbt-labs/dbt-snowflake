import dataclasses
from typing import Optional

from dbt_common.record import Record, Recorder


@dataclasses.dataclass
class CursorGetSqlStateParams:
    connection_name: str


@dataclasses.dataclass
class CursorGetSqlStateResult:
    msg: Optional[str]


@Recorder.register_record_type
class CursorGetSqlStateRecord(Record):
    params_cls = CursorGetSqlStateParams
    result_cls = CursorGetSqlStateResult
    group = "Database"
