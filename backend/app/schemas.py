"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str
    remark: str | None = None


class ReplayReceiptPayload(BaseModel):
    """重放终态回执：同一尝试只允许一个终态生效。"""
    segment_code: str
    attempt_no: int = 1
    outcome: str
    error_fragment: str = ""
    row_keys: list[str] = Field(default_factory=list)
    write_credential: str = ""


class StagedRowsPayload(BaseModel):
    """回填端暂存行与写入凭据。"""
    segment_code: str
    attempt_no: int = 1
    row_keys: list[str] = Field(default_factory=list)
    write_credential: str = ""


class HoldTerminalPayload(BaseModel):
    segment_code: str
    attempt_no: int = 1
    reason: str = "两份回执冲突，等待写入证据"


class ResolveTerminalPayload(BaseModel):
    segment_code: str
    attempt_no: int = 1


class ClaimPayload(BaseModel):
    worker: str = "值班"
