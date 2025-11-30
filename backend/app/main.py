from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uuid
from .models import DAGRequest, DAGResponse, DAGType
from .redis_client import redis_client

app = FastAPI(title="DAG Task Queue API (Pure Redis Stream)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.post("/api/dag/submit", response_model=DAGResponse)
async def submit_dag(request: DAGRequest):
    """Submit a DAG task to the Redis Stream queue"""
    job_id = str(uuid.uuid4())

    if request.dag_type == DAGType.SEQUENTIAL_ABC:
        dag_structure = {
            "tasks": ["A", "B", "C"],
            "dependencies": {
                "A": [],
                "B": ["A"],
                "C": ["B"]
            }
        }
    elif request.dag_type == DAGType.SKIP_B:
        dag_structure = {
            "tasks": ["A", "C"],
            "dependencies": {
                "A": [],
                "C": ["A"]
            }
        }
    elif request.dag_type == DAGType.PARALLEL_AB_THEN_C:
        dag_structure = {
            "tasks": ["A", "B", "C"],
            "dependencies": {
                "A": [],
                "B": [],
                "C": ["A", "B"]
            }
        }
    else:
        raise HTTPException(status_code=400, detail="Invalid DAG type")

    redis_client.set_job_status(job_id, {
        "dag_type": request.dag_type.value,
        "status": "queued",
        "tasks": {task: "pending" for task in dag_structure["tasks"]},
        "results": {}
    })

    redis_client.add_task(job_id, request.dag_type.value, dag_structure)

    return DAGResponse(
        job_id=job_id,
        dag_type=request.dag_type,
        message=f"DAG job {job_id} submitted successfully"
    )


@app.get("/api/dag/status/{job_id}")
async def get_dag_status(job_id: str):
    """Get the status of a DAG job"""
    status = redis_client.get_job_status(job_id)
    if not status:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": job_id, **status}


@app.get("/api/dag/jobs")
async def list_jobs():
    """List recent jobs"""
    keys = redis_client.client.keys("job:*")
    jobs = []
    for key in keys[:10]:
        job_id = key.replace("job:", "")
        status = redis_client.get_job_status(job_id)
        if status:
            jobs.append({"job_id": job_id, **status})
    return {"jobs": jobs}


@app.get("/api/queue/stats")
async def get_queue_stats():
    """큐 상태 조회"""
    try:
        stats = redis_client.get_queue_stats()
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
