import os
import signal
import sqlite3 as sql
import time

from forge import FORGE_PATH
from forge.db import JobStatus, finish_job, get_job


def handle_cancel(job_id: str) -> None:
    db_path = os.path.expanduser(f"{FORGE_PATH}/queue.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    con = sql.connect(db_path)

    try:
        job = get_job(con, job_id)
    except ValueError as e:
        con.close()
        print(str(e))
        return

    if job.status not in (JobStatus.RUNNING, JobStatus.QUEUED):
        print(f"Job {job.id} status is {JobStatus(job.status).name}; nothing to cancel")
        con.close()
        return

    if job.status == JobStatus.RUNNING:
        try:
            os.kill(job.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        except PermissionError:
            print(f"Failed to terminate process {job.pid} for job {job.id}; job left in RUNNING")
            con.close()
            return

        deadline = time.time() + 3.0
        while time.time() < deadline:
            try:
                os.kill(job.pid, 0)
            except ProcessLookupError:
                break
            except PermissionError:
                print(f"Failed to terminate process {job.pid} for job {job.id}; job left in RUNNING")
                con.close()
                return
            time.sleep(0.1)
        else:
            print(f"Failed to terminate process {job.pid} for job {job.id}; job left in RUNNING")
            con.close()
            return

    finish_job(
        con,
        job.id,
        JobStatus.CANCELED,
        None,
        int(time.time() * 1000),
        "Canceled by user",
    )
    con.close()
    print(f"Canceled job {job.id}")
