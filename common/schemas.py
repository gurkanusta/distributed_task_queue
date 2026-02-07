from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field, ConfigDict


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"
    RETRYING = "RETRYING"


class SubmitTaskRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str = Field(min_length=1, max_length=64)
    payload: Dict[str, Any] = Field(default_factory=dict)
    max_retries: int = Field(default=3, ge=0, le=20)
    timeout_seconds: int = Field(default=30, ge=1, le=3600)


class SubmitTaskResponse(BaseModel):
    task_id: str


class TaskView(BaseModel):
    task_id: str
    type: str
    payload: Dict[str, Any]
    status: TaskStatus
    retry_count: int
    max_retries: int
    timeout_seconds: int
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    assigned_worker_id: Optional[str] = None
    last_error: Optional[str] = None


class RegisterWorkerRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    worker_id: str = Field(min_length=3, max_length=64)


class RegisterWorkerResponse(BaseModel):
    ok: bool = True


class HeartbeatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    worker_id: str = Field(min_length=3, max_length=64)


class PullTaskResponse(BaseModel):
    task: Optional[TaskView] = None


class ReportResultRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    worker_id: str = Field(min_length=3, max_length=64)
    task_id: str
    ok: bool
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = Field(default=None, max_length=500)