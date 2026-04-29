import sqlite3 as sql
import time
import subprocess as sp
import os

from forge import FORGE_PATH
from forge.db import (
    Job,
    JobStatus,
    init_db,
    get_job,
    finish_job,
    start_job,
    batch_update_job_status,
    get_jobs_by_status,
)

class ForgeWorker():
    def __init__(self):
        self.con = sql.connect(os.path.expanduser(f"{FORGE_PATH}/queue.db"))
        self.poll_interval_seconds = 1.0
        
        init_db(self.con)

        one_week_ms = 7 * 24 * 60 * 60 * 1000
        stale_before_ms = int(time.time() * 1000) - one_week_ms

        stale_jobs = []
        stale_jobs.extend(
            get_jobs_by_status(
                self.con,
                JobStatus.QUEUED,
                created_at_to_ms=stale_before_ms,
            )
        )
        stale_jobs.extend(
            get_jobs_by_status(
                self.con,
                JobStatus.RUNNING,
                created_at_to_ms=stale_before_ms,
            )
        )

        stale_job_ids = [job.id for job in stale_jobs]
        batch_update_job_status(self.con, stale_job_ids, JobStatus.FAILED)

    def _run_job(self, job: Job) -> int:
        script_path = os.path.expanduser(job.script_path)
        submit_cwd = os.path.expanduser(job.submit_cwd)

        if os.access(script_path, os.X_OK):
            cmd = [script_path]
        else:
            cmd = ["bash", script_path]

        result = sp.run(cmd, cwd=submit_cwd)
        return result.returncode

    def run(self):
        while True:
            queued_jobs = get_jobs_by_status(self.con, JobStatus.QUEUED)
            if not queued_jobs:
                time.sleep(self.poll_interval_seconds)
                continue
            job = queued_jobs[0]

            start_job(self.con, job.id, int(time.time() * 1000))
            job = get_job(self.con, job.id)

            try:
                exit_code = self._run_job(job)
            except Exception as e:
                finish_job(
                    self.con,
                    job.id,
                    JobStatus.FAILED,
                    None,
                    int(time.time() * 1000),
                    str(e),
                )
                continue

            if exit_code == 0:
                finish_job(
                    self.con,
                    job.id,
                    JobStatus.SUCCEEDED,
                    exit_code,
                    int(time.time() * 1000),
                )
            else:
                finish_job(
                    self.con,
                    job.id,
                    JobStatus.FAILED,
                    exit_code,
                    int(time.time() * 1000),
                )
