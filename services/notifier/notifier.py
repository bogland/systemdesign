import os
import json
import time
import uuid
import asyncio
from typing import Set
import redis
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

STREAM = "completed_jobs"
GROUP = "notifiers"
CONSUMER = f"notifier-{uuid.uuid4().hex[:8]}"

app = FastAPI(title="Job Notifier Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Store for SSE clients
sse_clients: Set[asyncio.Queue] = set()


def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def ensure_consumer_group(client):
    """Create consumer group if not exists"""
    try:
        client.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        print(f"[Notifier] Created consumer group: {GROUP}")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
        print(f"[Notifier] Consumer group already exists: {GROUP}")


async def broadcast_to_clients(event_data: dict):
    """Send event to all connected SSE clients"""
    disconnected = set()
    for queue in sse_clients:
        try:
            await queue.put(event_data)
        except Exception:
            disconnected.add(queue)

    # Clean up disconnected clients
    for queue in disconnected:
        sse_clients.discard(queue)


async def redis_listener():
    """Background task to listen for completed jobs"""
    print(f"\n{'='*60}")
    print(f"Starting Notifier Service")
    print(f"{'='*60}")
    print(f"Consumer: {CONSUMER}")
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"{'='*60}\n")

    # Wait for Redis
    redis_client = None
    while True:
        try:
            redis_client = get_redis_client()
            redis_client.ping()
            print("[Notifier] Connected to Redis!")
            break
        except Exception as e:
            print(f"[Notifier] Waiting for Redis... ({e})")
            await asyncio.sleep(2)

    ensure_consumer_group(redis_client)

    print(f"\n[{CONSUMER}] Ready. Waiting for completed jobs...\n")

    while True:
        try:
            messages = redis_client.xreadgroup(
                GROUP,
                CONSUMER,
                {STREAM: ">"},
                count=10,
                block=1000  # 1 second block for responsiveness
            )

            if messages:
                for stream_name, stream_messages in messages:
                    for msg_id, data in stream_messages:
                        try:
                            event = {
                                "type": "job_completed",
                                "job_id": data.get("job_id"),
                                "dag_type": data.get("dag_type"),
                                "completed_at": data.get("completed_at"),
                                "message": f"Job {data.get('job_id')} completed!"
                            }

                            print(f"[Notifier] Broadcasting: {event['job_id']}")
                            await broadcast_to_clients(event)

                            redis_client.xack(STREAM, GROUP, msg_id)
                        except Exception as e:
                            print(f"[Notifier] Error processing: {e}")

            await asyncio.sleep(0.1)  # Small delay to prevent CPU spinning

        except redis.exceptions.ConnectionError as e:
            print(f"[Notifier] Redis connection error: {e}")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"[Notifier] Unexpected error: {e}")
            await asyncio.sleep(1)


@app.on_event("startup")
async def startup_event():
    """Start the Redis listener on app startup"""
    asyncio.create_task(redis_listener())


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "notifier"}


@app.get("/events")
async def sse_events(request: Request):
    """SSE endpoint for real-time notifications"""
    queue = asyncio.Queue()
    sse_clients.add(queue)

    print(f"[Notifier] SSE client connected. Total clients: {len(sse_clients)}")

    async def event_generator():
        try:
            # Send initial connection message
            yield {
                "event": "connected",
                "data": json.dumps({"message": "Connected to notification service"})
            }

            while True:
                # Check if client disconnected
                if await request.is_disconnected():
                    break

                try:
                    # Wait for new events with timeout
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield {
                        "event": event.get("type", "message"),
                        "data": json.dumps(event)
                    }
                except asyncio.TimeoutError:
                    # Send keepalive
                    yield {
                        "event": "keepalive",
                        "data": json.dumps({"timestamp": time.time()})
                    }
        finally:
            sse_clients.discard(queue)
            print(f"[Notifier] SSE client disconnected. Total clients: {len(sse_clients)}")

    return EventSourceResponse(event_generator())


@app.get("/events/{job_id}")
async def sse_job_events(request: Request, job_id: str):
    """SSE endpoint for specific job notifications"""
    queue = asyncio.Queue()
    sse_clients.add(queue)

    async def event_generator():
        try:
            yield {
                "event": "connected",
                "data": json.dumps({"message": f"Listening for job {job_id}"})
            }

            while True:
                if await request.is_disconnected():
                    break

                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)
                    # Filter by job_id
                    if event.get("job_id") == job_id:
                        yield {
                            "event": event.get("type", "message"),
                            "data": json.dumps(event)
                        }
                except asyncio.TimeoutError:
                    yield {
                        "event": "keepalive",
                        "data": json.dumps({"timestamp": time.time()})
                    }
        finally:
            sse_clients.discard(queue)

    return EventSourceResponse(event_generator())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
