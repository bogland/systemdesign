import time
import os
import json
import concurrent.futures
from typing import Dict, List, Any
import redis

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))


def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def update_task_status(job_id: str, task_name: str, status: str, result: Dict = None):
    """Update individual task status in Redis"""
    client = get_redis_client()

    # Get current job data
    job_data = client.hgetall(f"job:{job_id}")

    # Parse existing tasks
    tasks = {}
    if "tasks" in job_data:
        try:
            tasks = json.loads(job_data["tasks"])
        except:
            tasks = {}

    # Update this task's status
    tasks[task_name] = status

    # Parse existing results
    results = {}
    if "results" in job_data:
        try:
            results = json.loads(job_data["results"])
        except:
            results = {}

    # Update results if provided
    if result:
        results[task_name] = result

    # Determine overall job status
    all_completed = all(s == "completed" for s in tasks.values())
    any_running = any(s == "running" for s in tasks.values())

    if all_completed:
        job_status = "completed"
    elif any_running:
        job_status = "processing"
    else:
        job_status = "processing"

    # Save back to Redis
    client.hset(f"job:{job_id}", mapping={
        "status": job_status,
        "tasks": json.dumps(tasks),
        "results": json.dumps(results)
    })

    # Job 완료 시 completed_jobs Stream에 추가
    if all_completed:
        dag_type = job_data.get("dag_type", "unknown")
        client.xadd("completed_jobs", {
            "job_id": job_id,
            "dag_type": dag_type,
            "results": json.dumps(results),
            "completed_at": time.strftime("%Y-%m-%d %H:%M:%S")
        })
        print(f"[Job {job_id}] Added to completed_jobs stream")


def task_a(job_id: str) -> Dict[str, Any]:
    """Execute Task A"""
    update_task_status(job_id, "A", "running")
    print(f"[Job {job_id}] Executing Task A...")
    time.sleep(2)  # Simulate work
    result = {"task": "A", "status": "completed", "output": "Task A output data"}
    update_task_status(job_id, "A", "completed", result)
    print(f"[Job {job_id}] Task A completed!")
    return result


def task_b(job_id: str) -> Dict[str, Any]:
    """Execute Task B"""
    update_task_status(job_id, "B", "running")
    print(f"[Job {job_id}] Executing Task B...")
    time.sleep(2)  # Simulate work
    result = {"task": "B", "status": "completed", "output": "Task B output data"}
    update_task_status(job_id, "B", "completed", result)
    print(f"[Job {job_id}] Task B completed!")
    return result


def task_c(job_id: str, dependencies: List[Dict] = None) -> Dict[str, Any]:
    """Execute Task C"""
    update_task_status(job_id, "C", "running")
    print(f"[Job {job_id}] Executing Task C...")
    if dependencies:
        print(f"[Job {job_id}] Task C received inputs from: {[d['task'] for d in dependencies]}")
    time.sleep(2)  # Simulate work
    result = {"task": "C", "status": "completed", "output": "Task C final output"}
    update_task_status(job_id, "C", "completed", result)
    print(f"[Job {job_id}] Task C completed!")
    return result


def sequential_abc(job_id: str) -> Dict[str, Any]:
    """A -> B -> C sequential execution"""
    print(f"[Job {job_id}] Starting Sequential ABC (A -> B -> C)")

    result_a = task_a(job_id)
    result_b = task_b(job_id)
    result_c = task_c(job_id, dependencies=[result_a, result_b])

    return {
        "job_id": job_id,
        "flow_type": "sequential_abc",
        "results": {
            "A": result_a,
            "B": result_b,
            "C": result_c
        },
        "status": "completed"
    }


def skip_b(job_id: str) -> Dict[str, Any]:
    """A -> C execution (skipping B)"""
    print(f"[Job {job_id}] Starting Skip B (A -> C)")

    result_a = task_a(job_id)
    result_c = task_c(job_id, dependencies=[result_a])

    return {
        "job_id": job_id,
        "flow_type": "skip_b",
        "results": {
            "A": result_a,
            "C": result_c
        },
        "status": "completed"
    }


def parallel_ab_then_c(job_id: str) -> Dict[str, Any]:
    """A,B (parallel) -> C execution"""
    print(f"[Job {job_id}] Starting Parallel AB Then C (A,B parallel -> C)")

    # Execute A and B in parallel using ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_a = executor.submit(task_a, job_id)
        future_b = executor.submit(task_b, job_id)

        result_a = future_a.result()
        result_b = future_b.result()

    # Execute C after A and B complete
    result_c = task_c(job_id, dependencies=[result_a, result_b])

    return {
        "job_id": job_id,
        "flow_type": "parallel_ab",
        "results": {
            "A": result_a,
            "B": result_b,
            "C": result_c
        },
        "status": "completed"
    }


def execute_dag(job_id: str, dag_type: str) -> Dict[str, Any]:
    """Execute the appropriate DAG based on type"""
    if dag_type == "sequential_abc":
        return sequential_abc(job_id)
    elif dag_type == "skip_b":
        return skip_b(job_id)
    elif dag_type == "parallel_ab":
        return parallel_ab_then_c(job_id)
    else:
        raise ValueError(f"Unknown DAG type: {dag_type}")
