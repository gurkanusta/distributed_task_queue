from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Optional


@dataclass
class WorkerInfo:
    worker_id: str
    last_heartbeat: datetime
    in_flight: int = 0


class WorkerRegistry:
    def __init__(self, dead_after_seconds: int = 15):
        self._lock = asyncio.Lock()
        self._workers: Dict[str, WorkerInfo] = {}
        self.dead_after = timedelta(seconds=dead_after_seconds)

    async def register(self, worker_id: str) -> None:
        async with self._lock:
            self._workers[worker_id] = WorkerInfo(worker_id=worker_id, last_heartbeat=datetime.utcnow(), in_flight=0)

    async def heartbeat(self, worker_id: str) -> None:
        async with self._lock:
            w = self._workers.get(worker_id)
            if w:
                w.last_heartbeat = datetime.utcnow()

    async def mark_in_flight(self, worker_id: str, delta: int) -> None:
        async with self._lock:
            w = self._workers.get(worker_id)
            if w:
                w.in_flight = max(0, w.in_flight + delta)

    async def get_least_busy_alive(self) -> Optional[str]:
        now = datetime.utcnow()
        async with self._lock:
            alive = [w for w in self._workers.values() if now - w.last_heartbeat <= self.dead_after]
            if not alive:
                return None
            alive.sort(key=lambda x: (x.in_flight, x.last_heartbeat))
            return alive[0].worker_id

    async def dead_workers(self) -> list[str]:
        now = datetime.utcnow()
        async with self._lock:
            return [w.worker_id for w in self._workers.values() if now - w.last_heartbeat > self.dead_after]

    async def stats(self) -> dict:
        now = datetime.utcnow()
        async with self._lock:
            total = len(self._workers)
            alive = sum(1 for w in self._workers.values() if now - w.last_heartbeat <= self.dead_after)
            inflight = sum(w.in_flight for w in self._workers.values())
        return {"workers_total": total, "workers_alive": alive, "in_flight_total": inflight}