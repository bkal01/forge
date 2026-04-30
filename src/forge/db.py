import uuid
import time
import sqlite3 as sql

from enum import IntEnum

from forge import FORGE_PATH

class JobStatus(IntEnum):
    CREATED = 0
    QUEUED = 1
    RUNNING = 2
    SUCCEEDED = 3
    FAILED = 4
    CANCELED = 5

class Job():
    def __init__(
        self,
        script_path,
        submit_cwd,
        id=None,
        status=None,
    ):
        self.id = id or str(uuid.uuid4())
        self.script_path = script_path
        self.submit_cwd = submit_cwd
        self.status = JobStatus(status) or JobStatus.CREATED
    
    @staticmethod
    def from_row(row):
        job = Job(
            id=row[0],
            script_path=row[1],
            submit_cwd=row[2],
            status=row[3],
        )
        job.exit_code = row[4]
        job.error_message = row[5]
        job.created_at_ms = row[6]
        job.started_at_ms = row[7]
        job.finished_at_ms = row[8]
        job.pid = row[9]
        return job

create_table_query = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    script_path TEXT NOT NULL,
    submit_cwd TEXT NOT NULL,
    status INTEGER NOT NULL,
    exit_code INTEGER,
    error_message TEXT,
    created_at_ms INTEGER NOT NULL,
    started_at_ms INTEGER,
    finished_at_ms INTEGER,
    pid INTEGER NOT NULL DEFAULT 0
)"""

next_queued_job_index = """
CREATE INDEX IF NOT EXISTS next_queued ON jobs(status, created_at_ms)
"""

def init_db(con):
    cur = con.cursor()

    cur.execute(create_table_query)
    cur.execute(next_queued_job_index)
    con.commit()

def get_job(con, job_uuid):
    cur = con.cursor()

    res = cur.execute("SELECT * FROM jobs WHERE id = ?", (job_uuid,))
    row  = res.fetchone()
    if row is None:
        raise ValueError(f"Job with id {job_uuid} does not exist")
    return Job.from_row(row)

def get_jobs_by_status(
    con,
    status,
    created_at_from_ms=None,
    created_at_to_ms=None,
):
    cur = con.cursor()

    if isinstance(status, (list, tuple, set)):
        statuses = [int(s) for s in status]
    else:
        statuses = [int(status)]

    placeholders = ", ".join("?" for _ in statuses)
    query = f"SELECT * FROM jobs WHERE status IN ({placeholders})"
    params = list(statuses)

    if created_at_from_ms is not None:
        query += " AND created_at_ms >= ?"
        params.append(created_at_from_ms)

    if created_at_to_ms is not None:
        query += " AND created_at_ms <= ?"
        params.append(created_at_to_ms)

    query += " ORDER BY created_at_ms ASC"

    res = cur.execute(query, tuple(params))
    rows = res.fetchall()
    return [Job.from_row(row) for row in rows]

def batch_update_job_status(con, job_ids, status):
    if not job_ids:
        return 0

    cur = con.cursor()
    cur.executemany(
        "UPDATE jobs SET status = ? WHERE id = ?",
        [(int(status), job_id) for job_id in job_ids],
    )
    con.commit()
    return cur.rowcount

def start_job(con, job_id, started_at_ms):
    cur = con.cursor()
    cur.execute(
        "UPDATE jobs SET status = ?, started_at_ms = ? WHERE id = ?",
        (int(JobStatus.RUNNING), started_at_ms, job_id),
    )
    con.commit()
    return cur.rowcount


def set_job_pid(con, job_id, pid):
    cur = con.cursor()
    cur.execute("UPDATE jobs SET pid = ? WHERE id = ?", (pid, job_id))
    con.commit()
    return cur.rowcount

def finish_job(con, job_id, status, exit_code, finished_at_ms, error_message=None):
    cur = con.cursor()
    cur.execute(
        """
        UPDATE jobs
        SET status = ?, exit_code = ?, finished_at_ms = ?, error_message = ?, pid = 0
        WHERE id = ?
        """,
        (int(status), exit_code, finished_at_ms, error_message, job_id),
    )
    con.commit()
    return cur.rowcount
