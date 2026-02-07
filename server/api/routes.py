from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from common.schemas import (
    SubmitTaskRequest, SubmitTaskResponse, TaskView,
    RegisterWorkerRequest, RegisterWorkerResponse,
    HeartbeatRequest, PullTaskResponse, ReportResultRequest
)
from server.security.auth import require_client_key, require_worker_key
from server.tasks.manager import TaskManager


def task_to_view(t) -> TaskView:
    return TaskView(
        task_id=t.task_id,
        type=t.type,
        payload=t.payload,
        status=t.status,
        retry_count=t.retry_count,
        max_retries=t.max_retries,
        timeout_seconds=t.timeout_seconds,
        created_at=t.created_at,
        started_at=t.started_at,
        finished_at=t.finished_at,
        assigned_worker_id=t.assigned_worker_id,
        last_error=t.last_error,
    )


def build_router(mgr: TaskManager) -> APIRouter:
    r = APIRouter()


    @r.post("/client/tasks", response_model=SubmitTaskResponse, dependencies=[Depends(require_client_key)])
    async def submit_task(req: SubmitTaskRequest):
        task_id = await mgr.submit(req.type, req.payload, req.max_retries, req.timeout_seconds)
        return SubmitTaskResponse(task_id=task_id)

    @r.get("/client/tasks/{task_id}", response_model=TaskView, dependencies=[Depends(require_client_key)])
    async def get_task(task_id: str):
        t = await mgr.get(task_id)
        if not t:
            raise HTTPException(status_code=404, detail="Task not found")
        return task_to_view(t)

    @r.get("/client/tasks/{task_id}/result", dependencies=[Depends(require_client_key)])
    async def get_result(task_id: str):
        t = await mgr.get(task_id)
        if not t:
            raise HTTPException(status_code=404, detail="Task not found")
        return {"status": t.status.value, "result": t.result, "error": t.last_error}

    # -------- Worker API --------
    @r.post("/worker/register", response_model=RegisterWorkerResponse, dependencies=[Depends(require_worker_key)])
    async def register(req: RegisterWorkerRequest):
        await mgr.registry.register(req.worker_id)
        return RegisterWorkerResponse(ok=True)

    @r.post("/worker/heartbeat", dependencies=[Depends(require_worker_key)])
    async def heartbeat(req: HeartbeatRequest):
        await mgr.registry.heartbeat(req.worker_id)
        return {"ok": True}

    @r.post("/worker/pull", response_model=PullTaskResponse, dependencies=[Depends(require_worker_key)])
    async def pull(req: HeartbeatRequest):
        # reuse HeartbeatRequest to carry worker_id
        await mgr.registry.heartbeat(req.worker_id)
        t = await mgr.pull_for_worker(req.worker_id)
        return PullTaskResponse(task=task_to_view(t) if t else None)

    @r.post("/worker/report", dependencies=[Depends(require_worker_key)])
    async def report(req: ReportResultRequest):
        await mgr.report(req.worker_id, req.task_id, req.ok, req.result, req.error)
        return {"ok": True}


    @r.get("/metrics")
    async def metrics():

        raise HTTPException(status_code=404, detail="Use /client/metrics")

    @r.get("/client/metrics", dependencies=[Depends(require_client_key)])
    async def client_metrics():
        return await mgr.metrics()

    return r