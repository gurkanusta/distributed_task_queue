# Distributed Task Queue (Mini Celery)

This project is a **distributed task queue system** built from scratch using Python and FastAPI.

It demonstrates how long-running or resource-intensive jobs can be executed safely in the background using independent workers, instead of blocking HTTP requests.

The system is inspired by Celery but intentionally simplified to focus on **core distributed systems concepts**.

---

## Overview

The system consists of three components:

- **Task Server**  
  Accepts tasks, manages the queue, tracks task state, retries, and worker health.

- **Workers**  
  Pull tasks from the server, execute them, and report results or failures.

- **Client**  
  Submits tasks and queries their status and results.

Workers use a **pull-based model**, which provides natural load balancing and fault tolerance.

---

## Task Execution Model

Each task moves through the following states:

- PENDING
- RUNNING
- DONE
- RETRYING
- FAILED

Tasks are assigned to workers using a **lease-based mechanism**.  
If a worker crashes or becomes unresponsive, the task lease expires and the task is safely returned to the queue.  
This prevents duplicate execution and stuck tasks.

---

## Reliability & Safety

- Lease-based in-flight task tracking
- Automatic retries with backoff
- Timeout handling for long-running tasks
- Dead worker detection
- Concurrency-safe state management

---

## Security

- API key authentication
- Separate permissions for clients and workers
- Strict input validation
- Workers can only report results for tasks they own

---

## Technologies

- Python
- FastAPI
- asyncio
- HTTP (REST)
- Pydantic
- In-memory queue (designed to be replaceable with Redis)

---

## Use Cases

This type of system is commonly used for:

- Background job processing
- Email and notification systems
- Report and file generation
- Data processing pipelines
- AI / ML task execution

---

## Running the Project

Start the server:
```bash
python -m uvicorn server.main:app --reload
Start a worker:

python -m worker.worker

Submit a task:

python -m client.client
