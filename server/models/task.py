from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional

from common.schemas import TaskStatus


@dataclass
class Task:
    task_id: str
    type: str
    payload: Dict[str, Any]

    status: TaskStatus = TaskStatus.PENDING
    retry_count: int = 0
    max_retries: int = 3
    timeout_seconds: int = 30

    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    assigned_worker_id: Optional[str] = None

    result: Optional[Dict[str, Any]] = None
    last_error: Optional[str] = None

    def mark_running(self, worker_id: str) -> None:
        self.status = TaskStatus.RUNNING
        self.started_at = datetime.utcnow()
        self.assigned_worker_id = worker_id
        self.last_error = None

    def mark_done(self, result: Dict[str, Any] | None) -> None:
        self.status = TaskStatus.DONE
        self.finished_at = datetime.utcnow()
        self.result = result or {}

    def mark_failed(self, error: str) -> None:
        self.status = TaskStatus.FAILED
        self.finished_at = datetime.utcnow()
        self.last_error = error

    def mark_retrying(self, error: str) -> None:
        self.status = TaskStatus.RETRYING
        self.last_error = error
        self.assigned_worker_id = None
        self.started_at = None