from __future__ import annotations

import os
from fastapi import Header, HTTPException, status

CLIENT_API_KEY = os.getenv("DTQ_CLIENT_API_KEY", "client-dev-key")
WORKER_API_KEY = os.getenv("DTQ_WORKER_API_KEY", "worker-dev-key")


def require_client_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    if not x_api_key or x_api_key != CLIENT_API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid client API key")


def require_worker_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    if not x_api_key or x_api_key != WORKER_API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid worker API key")