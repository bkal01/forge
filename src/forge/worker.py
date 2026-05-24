import sqlite3 as sql
import time
import subprocess as sp
import os
import signal
import threading
import uuid

from forge import FORGE_PATH
from forge.db import (
    Job,
    JobStatus,
    init_db,
    get_job,
    finish_job,
    start_job,
    set_job_pid,
    get_jobs_by_status,
    requeue_running_jobs,
)
from forge.logs import ForgeLogger

class ForgeWorker():
    def __init__(self):
        state_dir = os.path.expanduser(FORGE_PATH)
        os.makedirs(state_dir, exist_ok=True)

        self.con = sql.connect(os.path.expanduser(f"{FORGE_PATH}/queue.db"))
        self.poll_interval_seconds = 1.0
        self.run_id = str(uuid.uuid4())
        self.logger = ForgeLogger(run_id=self.run_id)

        init_db(self.con)
        self.logger.system("INFO", "worker_started", "Forge worker started")

        # requeue previously running jobs that are hung due to worker restart
        running_jobs = get_jobs_by_status(self.con, JobStatus.RUNNING)
        for job in running_jobs:
            if job.pid:
                try:
                    os.killpg(job.pid, signal.SIGTERM)
                    self.logger.system(
                        "WARN",
                        "interrupted_job_process_terminated",
                        "Terminated process group for interrupted job",
                        job_id=job.id,
                        pid=job.pid,
                    )
                except ProcessLookupError:
                    pass
                except PermissionError as e:
                    self.logger.system(
                        "WARN",
                        "interrupted_job_process_termination_failed",
                        "Failed to terminate process group for interrupted job",
                        job_id=job.id,
                        pid=job.pid,
                        error=str(e),
                    )

        requeued_count = requeue_running_jobs(self.con)
        if requeued_count:
            self.logger.system(
                "WARN",
                "interrupted_jobs_requeued",
                "Requeued jobs left RUNNING by previous worker",
                count=requeued_count,
            )

    def _run_job(self, job: Job) -> int:
        script_path = os.path.expanduser(job.script_path)
        submit_cwd = os.path.expanduser(job.submit_cwd)
        job_logger = self.logger.for_job(job.id)
        started_at = int(time.time() * 1000)

        if os.access(script_path, os.X_OK):
            cmd = [script_path]
        else:
            cmd = ["bash", script_path]

        job_logger.worker(
            "INFO",
            "job_started",
            "Starting job process",
            script_path=script_path,
            submit_cwd=submit_cwd,
            command=cmd,
        )

        proc = sp.Popen(
            cmd,
            cwd=submit_cwd,
            stdout=sp.PIPE,
            stderr=sp.PIPE,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        set_job_pid(self.con, job.id, proc.pid)
        job_logger.worker("INFO", "job_pid", "Job process started", pid=proc.pid)

        def stream_output(pipe, stream_name: str):
            try:
                for line in iter(pipe.readline, ""):
                    job_logger.output(stream_name, line.rstrip("\n"))
            finally:
                pipe.close()

        stdout_t = threading.Thread(target=stream_output, args=(proc.stdout, "stdout"))
        stderr_t = threading.Thread(target=stream_output, args=(proc.stderr, "stderr"))
        stdout_t.start()
        stderr_t.start()

        exit_code = proc.wait()
        stdout_t.join()
        stderr_t.join()

        job_logger.worker(
            "INFO",
            "job_finished",
            "Job process exited",
            exit_code=exit_code,
            duration_ms=int(time.time() * 1000) - started_at,
        )
        return exit_code

    def run(self):
        while True:
            queued_jobs = get_jobs_by_status(self.con, JobStatus.QUEUED)
            if not queued_jobs:
                time.sleep(self.poll_interval_seconds)
                continue
            job = queued_jobs[0]
            self.logger.system(
                "INFO",
                "job_picked",
                "Picked queued job",
                job_id=job.id,
                queue_depth=len(queued_jobs),
            )
            self.logger.for_job(job.id).worker("INFO", "job_picked", "Picked queued job")

            start_job(self.con, job.id, int(time.time() * 1000))
            self.logger.system(
                "INFO",
                "job_state_changed",
                "Job state updated",
                job_id=job.id,
                status_from="QUEUED",
                status_to="RUNNING",
            )
            job = get_job(self.con, job.id)

            try:
                exit_code = self._run_job(job)
            except Exception as e:
                self.logger.system(
                    "ERROR",
                    "job_exception",
                    "Job execution raised exception",
                    job_id=job.id,
                    error=str(e),
                )
                self.logger.for_job(job.id).worker(
                    "ERROR",
                    "job_exception",
                    "Job execution raised exception",
                    error=str(e),
                )
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
                self.logger.system(
                    "INFO",
                    "job_state_changed",
                    "Job state updated",
                    job_id=job.id,
                    status_from="RUNNING",
                    status_to="SUCCEEDED",
                    exit_code=exit_code,
                )
            else:
                finish_job(
                    self.con,
                    job.id,
                    JobStatus.FAILED,
                    exit_code,
                    int(time.time() * 1000),
                )
                self.logger.system(
                    "WARN",
                    "job_state_changed",
                    "Job state updated",
                    job_id=job.id,
                    status_from="RUNNING",
                    status_to="FAILED",
                    exit_code=exit_code,
                )
