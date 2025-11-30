import os

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_STREAM_NAME = "dag_tasks"
REDIS_CONSUMER_GROUP = "dag_workers"
