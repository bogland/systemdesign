import os
import json
import time
import uuid
import redis
from tasks import execute_dag

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_STREAM_NAME = "dag_tasks"
REDIS_CONSUMER_GROUP = "dag_workers"
CONSUMER_NAME = os.getenv("CONSUMER_NAME", f"worker-{uuid.uuid4().hex[:8]}")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1))
CLAIM_TIMEOUT = int(os.getenv("CLAIM_TIMEOUT", 60000))  # 60초


def get_redis_client():
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True
    )


def ensure_consumer_group(client):
    """Create consumer group if it doesn't exist"""
    try:
        client.xgroup_create(
            REDIS_STREAM_NAME,
            REDIS_CONSUMER_GROUP,
            id="0",
            mkstream=True
        )
        print(f"Created consumer group: {REDIS_CONSUMER_GROUP}")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
        print(f"Consumer group already exists: {REDIS_CONSUMER_GROUP}")


def update_job_status(client, job_id: str, status: dict):
    """Update job status in Redis"""
    status["worker"] = CONSUMER_NAME
    status["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    client.hset(f"job:{job_id}", mapping={
        k: json.dumps(v) if isinstance(v, (dict, list)) else str(v)
        for k, v in status.items()
    })
    client.expire(f"job:{job_id}", 3600)  # 1 hour TTL


def claim_pending_messages(client):
    """
    다른 Worker가 처리하다 실패한 메시지를 가져옴 (장애 복구)
    """
    try:
        pending = client.xpending_range(
            REDIS_STREAM_NAME,
            REDIS_CONSUMER_GROUP,
            min="-",
            max="+",
            count=10
        )

        for msg in pending:
            message_id = msg["message_id"]
            idle_time = msg["time_since_delivered"]

            if idle_time > CLAIM_TIMEOUT:
                claimed = client.xclaim(
                    REDIS_STREAM_NAME,
                    REDIS_CONSUMER_GROUP,
                    CONSUMER_NAME,
                    min_idle_time=CLAIM_TIMEOUT,
                    message_ids=[message_id]
                )
                if claimed:
                    print(f"[{CONSUMER_NAME}] Claimed stale message: {message_id}")
                    return claimed
    except Exception as e:
        print(f"Error claiming pending messages: {e}")

    return None


def process_message(client, message_id: str, message_data: dict):
    """Process a single message from the stream"""
    job_id = message_data.get("job_id")
    dag_type = message_data.get("dag_type")

    print(f"\n{'='*50}")
    print(f"[{CONSUMER_NAME}] Processing Job: {job_id}")
    print(f"[{CONSUMER_NAME}] DAG Type: {dag_type}")
    print(f"{'='*50}\n")

    try:
        update_job_status(client, job_id, {
            "status": "running",
            "dag_type": dag_type
        })

        # Execute the DAG (pure Python, no Prefect)
        result = execute_dag(job_id, dag_type)

        update_job_status(client, job_id, {
            "status": "completed",
            "dag_type": dag_type,
            "results": result.get("results", {}),
            "tasks": {task: "completed" for task in result.get("results", {}).keys()}
        })

        print(f"\n[{CONSUMER_NAME}] Job {job_id} completed successfully!")

    except Exception as e:
        print(f"\n[{CONSUMER_NAME}] Job {job_id} Error: {str(e)}")
        update_job_status(client, job_id, {
            "status": "failed",
            "error": str(e)
        })

    client.xack(REDIS_STREAM_NAME, REDIS_CONSUMER_GROUP, message_id)
    print(f"[{CONSUMER_NAME}] ACK message: {message_id}")


def main():
    print(f"\n{'='*60}")
    print(f"Starting DAG Worker (Pure Redis Stream - No Prefect)")
    print(f"{'='*60}")
    print(f"Worker Name: {CONSUMER_NAME}")
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"Stream: {REDIS_STREAM_NAME}")
    print(f"Consumer Group: {REDIS_CONSUMER_GROUP}")
    print(f"Batch Size: {BATCH_SIZE}")
    print(f"Claim Timeout: {CLAIM_TIMEOUT}ms")
    print(f"{'='*60}\n")

    client = get_redis_client()

    while True:
        try:
            client.ping()
            print("Connected to Redis!")
            break
        except redis.exceptions.ConnectionError:
            print("Waiting for Redis...")
            time.sleep(2)

    ensure_consumer_group(client)

    print(f"\n[{CONSUMER_NAME}] Ready. Waiting for tasks...\n")

    while True:
        try:
            # 1. 먼저 실패한 Worker의 pending 메시지 확인
            claimed = claim_pending_messages(client)
            if claimed:
                for message_id, message_data in claimed:
                    process_message(client, message_id, message_data)
                continue

            # 2. 새로운 메시지 읽기
            messages = client.xreadgroup(
                REDIS_CONSUMER_GROUP,
                CONSUMER_NAME,
                {REDIS_STREAM_NAME: ">"},
                count=BATCH_SIZE,
                block=5000
            )

            if messages:
                for stream_name, stream_messages in messages:
                    for message_id, message_data in stream_messages:
                        process_message(client, message_id, message_data)

        except redis.exceptions.ConnectionError as e:
            print(f"[{CONSUMER_NAME}] Redis connection error: {e}")
            time.sleep(5)
        except KeyboardInterrupt:
            print(f"\n[{CONSUMER_NAME}] Shutting down...")
            break
        except Exception as e:
            print(f"[{CONSUMER_NAME}] Unexpected error: {e}")
            time.sleep(1)


if __name__ == "__main__":
    main()
