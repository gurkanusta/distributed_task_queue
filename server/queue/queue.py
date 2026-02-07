from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime, timedelta
from typing import Deque, Dict, Optional, Tuple


class InMemoryLeaseQueue:

    def __init__(self):
        self._lock = asyncio.Lock()
        self._ready: Deque[str] = deque()
        self._inflight: Dict[str, Tuple[str, datetime]] = {}
        self._ready_set: set[str] = set()

    async def push_ready(self, task_id: str) -> None:
        async with self._lock:
            if task_id in self._inflight:
                return
            if task_id in self._ready_set:
                return
            self._ready.append(task_id)
            self._ready_set.add(task_id)

    async def lease(self, worker_id: str, lease_seconds: int) -> Optional[str]:
        lease_until = datetime.utcnow() + timedelta(seconds=lease_seconds)
        async with self._lock:
            if not self._ready:
                return None
            task_id = self._ready.popleft()
            self._ready_set.discard(task_id)

            self._inflight[task_id] = (worker_id, lease_until)
            return task_id

    async def ack(self, task_id: str, worker_id: str) -> bool:
        async with self._lock:
            cur = self._inflight.get(task_id)
            if not cur:
                return False
            w, _ = cur
            if w != worker_id:
                return False
            del self._inflight[task_id]
            return True

    async def release(self, task_id: str) -> None:

        async with self._lock:
            if task_id in self._inflight:
                del self._inflight[task_id]
        await self.push_ready(task_id)

    async def reap_expired_leases(self) -> list[str]:
        now = datetime.utcnow()
        expired: list[str] = []
        async with self._lock:
            for tid, (_, until) in list(self._inflight.items()):
                if now > until:
                    expired.append(tid)
                    del self._inflight[tid]

        for tid in expired:
            await self.push_ready(tid)
        return expired

    async def size_ready(self) -> int:
        async with self._lock:
            return len(self._ready)

    async def size_inflight(self) -> int:
        async with self._lock:
            return len(self._inflight)