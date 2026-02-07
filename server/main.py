from __future__ import annotations

import asyncio
import os
from fastapi import FastAPI

from server.api.routes import build_router
from server.queue.queue import InMemoryLeaseQueue
from server.tasks.manager import TaskManager
from server.workers.registry import WorkerRegistry


def create_app() -> FastAPI:
    dead_after = int(os.getenv("DTQ_WORKER_DEAD_AFTER_SECONDS", "15"))
    lease_seconds = int(os.getenv("DTQ_TASK_LEASE_SECONDS", "20"))

    registry = WorkerRegistry(dead_after_seconds=dead_after)
    queue = InMemoryLeaseQueue()
    mgr = TaskManager(registry=registry, queue=queue, lease_seconds=lease_seconds)

    app = FastAPI(title="Distributed Task Queue (Mini)", version="1.1.0")
    app.include_router(build_router(mgr))

    @app.on_event("startup")
    async def _startup():
        app.state._stop = False

        async def sweeper():
            while not app.state._stop:
                await mgr.timeout_and_dead_worker_sweeper()
                await asyncio.sleep(2)

        app.state._sweeper_task = asyncio.create_task(sweeper())

    @app.on_event("shutdown")
    async def _shutdown():
        app.state._stop = True
        t = getattr(app.state, "_sweeper_task", None)
        if t:
            t.cancel()

    return app


app = create_app()