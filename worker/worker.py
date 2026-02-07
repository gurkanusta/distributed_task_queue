from __future__ import annotations

import os
import time
import httpx

from worker.executor import execute

SERVER_BASE = os.getenv("DTQ_SERVER_BASE", "http://127.0.0.1:8000")
WORKER_ID = os.getenv("DTQ_WORKER_ID", "worker-1")
WORKER_KEY = os.getenv("DTQ_WORKER_API_KEY", "worker-dev-key")


def main():
    headers = {"X-API-Key": WORKER_KEY}
    with httpx.Client(timeout=10.0) as client:

        client.post(f"{SERVER_BASE}/worker/register", json={"worker_id": WORKER_ID}, headers=headers)

        while True:

            resp = client.post(f"{SERVER_BASE}/worker/pull", json={"worker_id": WORKER_ID}, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            task = data.get("task")

            idle = 0.2
            max_idle = 2.0

            if not task:
                time.sleep(idle)
                idle = min(max_idle, idle * 1.3)
                continue


            idle = 0.2

            task_id = task["task_id"]
            task_type = task["type"]
            payload = task["payload"]

            ok = True
            result = None
            error = None
            try:
                result = execute(task_type, payload)
            except Exception as e:
                ok = False
                error = str(e)

            client.post(
                f"{SERVER_BASE}/worker/report",
                json={"worker_id": WORKER_ID, "task_id": task_id, "ok": ok, "result": result, "error": error},
                headers=headers
            )


if __name__ == "__main__":
    main()