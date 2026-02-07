from __future__ import annotations

import os
import time
import httpx

SERVER_BASE = os.getenv("DTQ_SERVER_BASE", "http://127.0.0.1:8000")
CLIENT_KEY = os.getenv("DTQ_CLIENT_API_KEY", "client-dev-key")


def main():
    headers = {"X-API-Key": CLIENT_KEY}
    with httpx.Client(timeout=10.0) as c:

        r = c.post(
            f"{SERVER_BASE}/client/tasks",
            json={"type": "add", "payload": {"a": 10, "b": 32}, "max_retries": 3, "timeout_seconds": 10},
            headers=headers,
        )
        r.raise_for_status()
        task_id = r.json()["task_id"]
        print("Submitted:", task_id)

        # poll
        while True:
            st = c.get(f"{SERVER_BASE}/client/tasks/{task_id}", headers=headers).json()
            print("Status:", st["status"])
            if st["status"] in ("DONE", "FAILED"):
                res = c.get(f"{SERVER_BASE}/client/tasks/{task_id}/result", headers=headers).json()
                print("Result:", res)
                break
            time.sleep(0.5)


if __name__ == "__main__":
    main()