import redis
import json
from typing import Optional, Dict, Any
from .config import REDIS_HOST, REDIS_PORT, REDIS_STREAM_NAME, REDIS_CONSUMER_GROUP


class RedisStreamClient:
    def __init__(self):
        self.client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True
        )
        self._ensure_consumer_group()

    def _ensure_consumer_group(self):
        """Create consumer group if it doesn't exist"""
        try:
            self.client.xgroup_create(
                REDIS_STREAM_NAME,
                REDIS_CONSUMER_GROUP,
                id="0",
                mkstream=True
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def add_task(self, job_id: str, dag_type: str, task_data: Dict[str, Any]) -> str:
        """Add a task to the Redis Stream"""
        message = {
            "job_id": job_id,
            "dag_type": dag_type,
            "data": json.dumps(task_data)
        }
        message_id = self.client.xadd(REDIS_STREAM_NAME, message)
        return message_id

    def set_job_status(self, job_id: str, status: Dict[str, Any]):
        """Store job status in Redis"""
        self.client.hset(f"job:{job_id}", mapping={
            k: json.dumps(v) if isinstance(v, (dict, list)) else str(v)
            for k, v in status.items()
        })
        self.client.expire(f"job:{job_id}", 3600)  # 1 hour TTL

    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job status from Redis"""
        data = self.client.hgetall(f"job:{job_id}")
        if not data:
            return None
        result = {}
        for k, v in data.items():
            try:
                result[k] = json.loads(v)
            except (json.JSONDecodeError, TypeError):
                result[k] = v
        return result

    def get_queue_stats(self) -> Dict[str, Any]:
        """큐 상태 조회"""
        total = self.client.xlen(REDIS_STREAM_NAME)
        pending_info = self.client.xpending(REDIS_STREAM_NAME, REDIS_CONSUMER_GROUP)
        pending_count = pending_info.get("pending", 0) if pending_info else 0

        groups = self.client.xinfo_groups(REDIS_STREAM_NAME)
        group_info = None
        for g in groups:
            if g.get("name") == REDIS_CONSUMER_GROUP:
                group_info = g
                break

        last_delivered_id = group_info.get("last-delivered-id", "0-0") if group_info else "0-0"

        return {
            "total_in_stream": total,
            "pending": pending_count,
            "consumers": group_info.get("consumers", 0) if group_info else 0,
            "last_delivered_id": last_delivered_id
        }


redis_client = RedisStreamClient()
