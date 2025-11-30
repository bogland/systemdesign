import os
import json
import time
import uuid
import redis
import pymysql
from pymysql.cursors import DictCursor

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "daguser")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "dagpassword")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "dagqueue")

STREAM = "completed_jobs"
GROUP = "db_writers"
CONSUMER = f"db-writer-{uuid.uuid4().hex[:8]}"


def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def get_mysql_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        cursorclass=DictCursor,
        autocommit=True
    )


def init_database():
    """Create table if not exists"""
    conn = get_mysql_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS completed_jobs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    job_id VARCHAR(36) UNIQUE NOT NULL,
                    dag_type VARCHAR(50) NOT NULL,
                    results JSON,
                    completed_at DATETIME,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        print("[DB Writer] Table initialized")
    finally:
        conn.close()


def save_to_database(data: dict):
    """Save completed job to MySQL"""
    conn = get_mysql_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO completed_jobs (job_id, dag_type, results, completed_at)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    dag_type = VALUES(dag_type),
                    results = VALUES(results),
                    completed_at = VALUES(completed_at)
            """, (
                data["job_id"],
                data["dag_type"],
                data["results"],
                data["completed_at"]
            ))
        print(f"[DB Writer] Saved job {data['job_id']} to MySQL")
    finally:
        conn.close()


def ensure_consumer_group(client):
    """Create consumer group if not exists"""
    try:
        client.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        print(f"[DB Writer] Created consumer group: {GROUP}")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
        print(f"[DB Writer] Consumer group already exists: {GROUP}")


def main():
    print(f"\n{'='*60}")
    print(f"Starting DB Writer Service")
    print(f"{'='*60}")
    print(f"Consumer: {CONSUMER}")
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"MySQL: {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
    print(f"{'='*60}\n")

    # Wait for Redis
    redis_client = get_redis_client()
    while True:
        try:
            redis_client.ping()
            print("[DB Writer] Connected to Redis!")
            break
        except redis.exceptions.ConnectionError:
            print("[DB Writer] Waiting for Redis...")
            time.sleep(2)

    # Wait for MySQL
    while True:
        try:
            init_database()
            print("[DB Writer] Connected to MySQL!")
            break
        except Exception as e:
            print(f"[DB Writer] Waiting for MySQL... ({e})")
            time.sleep(2)

    ensure_consumer_group(redis_client)

    print(f"\n[{CONSUMER}] Ready. Waiting for completed jobs...\n")

    while True:
        try:
            messages = redis_client.xreadgroup(
                GROUP,
                CONSUMER,
                {STREAM: ">"},
                count=10,
                block=5000
            )

            if messages:
                for stream_name, stream_messages in messages:
                    for msg_id, data in stream_messages:
                        try:
                            save_to_database(data)
                            redis_client.xack(STREAM, GROUP, msg_id)
                        except Exception as e:
                            print(f"[DB Writer] Error saving job: {e}")
                            # Don't ACK - will be retried

        except redis.exceptions.ConnectionError as e:
            print(f"[DB Writer] Redis connection error: {e}")
            time.sleep(5)
        except KeyboardInterrupt:
            print(f"\n[{CONSUMER}] Shutting down...")
            break
        except Exception as e:
            print(f"[DB Writer] Unexpected error: {e}")
            time.sleep(1)


if __name__ == "__main__":
    main()
