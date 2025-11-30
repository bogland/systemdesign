from pydantic import BaseModel
from typing import Optional
from enum import Enum


class DAGType(str, Enum):
    SEQUENTIAL_ABC = "sequential_abc"  # A -> B -> C
    SKIP_B = "skip_b"                   # A -> C
    PARALLEL_AB_THEN_C = "parallel_ab"  # A,B (parallel) -> C


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class DAGRequest(BaseModel):
    dag_type: DAGType


class DAGResponse(BaseModel):
    job_id: str
    dag_type: DAGType
    message: str


class TaskResult(BaseModel):
    job_id: str
    task_name: str
    status: TaskStatus
    result: Optional[str] = None
