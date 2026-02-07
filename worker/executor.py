from __future__ import annotations

import time
from typing import Any, Dict


def execute(task_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:

    if task_type == "sleep":
        seconds = int(payload.get("seconds", 1))
        seconds = max(0, min(seconds, 30))
        time.sleep(seconds)
        return {"slept": seconds}

    if task_type == "add":
        a = float(payload.get("a", 0))
        b = float(payload.get("b", 0))
        return {"sum": a + b}

    if task_type == "echo":
        return {"echo": payload}

    # Unknown task type
    raise ValueError(f"Unknown task type: {task_type}")