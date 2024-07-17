import dataclasses
from typing import Optional

from dbt_common.record import Record, Recorder


@dataclasses.dataclass
class CursorGetSfqidParams:
    connection_name: str


@dataclasses.dataclass
class CursorGetSfqidResult:
    msg: Optional[str]


@Recorder.register_record_type
class CursorGetSfqidRecord(Record):
    params_cls = CursorGetSfqidParams
    result_cls = CursorGetSfqidResult
    group = "Database"
