from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Dict, Optional
from uuid import uuid4

from common.schemas import TaskStatus
from server.models.task import Task
from server.queue.queue import InMemoryLeaseQueue
from server.workers.registry import WorkerRegistry


class TaskManager:
    def __init__(self, registry: WorkerRegistry, queue: InMemoryLeaseQueue, lease_seconds: int = 20):
        self._lock = asyncio.Lock()
        self.registry = registry
        self.queue = queue
        self.lease_seconds = lease_seconds
        self.tasks: Dict[str, Task] = {}

    async def submit(self, type_: str, payload: dict, max_retries: int, timeout_seconds: int) -> str:
        task_id = str(uuid4())
        task = Task(
            task_id=task_id,
            type=type_,
            payload=payload,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
        )
        async with self._lock:
            self.tasks[task_id] = task
        await self.queue.push_ready(task_id)
        return task_id

    async def get(self, task_id: str) -> Optional[Task]:
        async with self._lock:
            return self.tasks.get(task_id)

    async def pull_for_worker(self, worker_id: str) -> Optional[Task]:

        task_id = await self.queue.lease(worker_id=worker_id, lease_seconds=self.lease_seconds)
        if not task_id:
            return None

        async with self._lock:
            task = self.tasks.get(task_id)
            if not task:
                # ack lease to avoid stuck inflight
                await self.queue.ack(task_id, worker_id)
                return None

            if task.status in (TaskStatus.DONE, TaskStatus.FAILED):
                # ack and ignore
                await self.queue.ack(task_id, worker_id)
                return None

            task.mark_running(worker_id)

        await self.registry.mark_in_flight(worker_id, +1)
        return task

    async def report(self, worker_id: str, task_id: str, ok: bool, result: dict | None, error: str | None) -> None:
        # only accept if lease exists for this worker
        leased_ok = await self.queue.ack(task_id, worker_id)
        if not leased_ok:
            return

        async with self._lock:
            task = self.tasks.get(task_id)
            if not task:
                return
            if task.assigned_worker_id != worker_id:
                return

            if ok:
                task.mark_done(result or {})
            else:
                task.retry_count += 1
                err = (error or "Unknown error")[:500]
                if task.retry_count <= task.max_retries:
                    task.status = TaskStatus.RETRYING
                    task.last_error = err
                    task.assigned_worker_id = None
                    task.started_at = None
                else:
                    task.mark_failed(err)

        await self.registry.mark_in_flight(worker_id, -1)


        t = await self.get(task_id)
        if t and t.status == TaskStatus.RETRYING:

            await asyncio.sleep(min(5.0, 0.5 * t.retry_count))
            async with self._lock:
                if t.status == TaskStatus.RETRYING:
                    t.status = TaskStatus.PENDING
            await self.queue.push_ready(task_id)

    async def timeout_and_dead_worker_sweeper(self) -> dict:

        expired = await self.queue.reap_expired_leases()
        now = datetime.utcnow()
        requeued = 0
        failed = 0

        async with self._lock:
            for tid in expired:
                task = self.tasks.get(tid)
                if not task:
                    continue

                if task.status == TaskStatus.RUNNING:
                    task.retry_count += 1
                    if task.retry_count <= task.max_retries:
                        task.status = TaskStatus.PENDING
                        task.assigned_worker_id = None
                        task.started_at = None
                        task.last_error = "Lease expired (worker lost/timeout)"
                        requeued += 1
                    else:
                        task.mark_failed("Lease expired and retry limit exceeded")
                        failed += 1

        return {"leases_expired": len(expired), "requeued": requeued, "failed": failed, "ts": now.isoformat()}

    async def metrics(self) -> dict:
        ready = await self.queue.size_ready()
        inflight = await self.queue.size_inflight()
        wstats = await self.registry.stats()
        async with self._lock:
            total = len(self.tasks)
            by_status = {}
            for t in self.tasks.values():
                by_status[t.status.value] = by_status.get(t.status.value, 0) + 1
        return {
            "queue_ready": ready,
            "queue_inflight": inflight,
            "tasks_total": total,
            "tasks_by_status": by_status,
            **wstats,
        }