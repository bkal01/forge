import os
import signal
import sqlite3 as sql
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from forge.db import JobStatus, get_job, init_db


def insert_job(con, job_id="job-1", status=JobStatus.RUNNING, pid=12345):
    con.execute(
        """
        INSERT INTO jobs (id, script_path, submit_cwd, status, created_at_ms, pid)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (job_id, "/tmp/job.sh", "/tmp", int(status), int(time.time() * 1000), pid),
    )
    con.commit()


class WorkerRecoveryTests(unittest.TestCase):
    def test_worker_startup_terminates_and_requeues_running_jobs(self):
        import forge.logs
        import forge.worker

        with tempfile.TemporaryDirectory() as state_dir:
            db_path = os.path.join(state_dir, "queue.db")
            con = sql.connect(db_path)
            init_db(con)
            insert_job(con, job_id="job-1", status=JobStatus.RUNNING, pid=12345)
            con.close()

            with mock.patch.object(forge.worker, "FORGE_PATH", state_dir), \
                 mock.patch.object(forge.logs, "FORGE_PATH", state_dir), \
                 mock.patch.object(forge.worker.os, "killpg") as killpg:
                worker = forge.worker.ForgeWorker()
                worker.con.close()

            killpg.assert_called_once_with(12345, signal.SIGTERM)

            con = sql.connect(db_path)
            job = get_job(con, "job-1")
            con.close()

            self.assertEqual(job.status, JobStatus.QUEUED)
            self.assertEqual(job.pid, 0)

    def test_worker_startup_requeues_running_jobs_without_pid(self):
        import forge.logs
        import forge.worker

        with tempfile.TemporaryDirectory() as state_dir:
            db_path = os.path.join(state_dir, "queue.db")
            con = sql.connect(db_path)
            init_db(con)
            insert_job(con, job_id="job-1", status=JobStatus.RUNNING, pid=0)
            con.close()

            with mock.patch.object(forge.worker, "FORGE_PATH", state_dir), \
                 mock.patch.object(forge.logs, "FORGE_PATH", state_dir), \
                 mock.patch.object(forge.worker.os, "killpg") as killpg:
                worker = forge.worker.ForgeWorker()
                worker.con.close()

            killpg.assert_not_called()

            con = sql.connect(db_path)
            job = get_job(con, "job-1")
            con.close()

            self.assertEqual(job.status, JobStatus.QUEUED)
            self.assertEqual(job.pid, 0)

    def test_run_job_starts_subprocess_in_new_session(self):
        import forge.logs
        import forge.worker
        from forge.db import Job

        class EmptyPipe:
            def readline(self):
                return ""

            def close(self):
                pass

        class FakeProc:
            pid = 23456
            stdout = EmptyPipe()
            stderr = EmptyPipe()

            def wait(self):
                return 0

        with tempfile.TemporaryDirectory() as state_dir:
            with mock.patch.object(forge.worker, "FORGE_PATH", state_dir), \
                 mock.patch.object(forge.logs, "FORGE_PATH", state_dir):
                worker = forge.worker.ForgeWorker()

                job = Job(
                    id="job-1",
                    script_path="/tmp/job.sh",
                    submit_cwd="/tmp",
                    status=JobStatus.RUNNING,
                )
                with mock.patch.object(forge.worker.sp, "Popen", return_value=FakeProc()) as popen:
                    exit_code = worker._run_job(job)

                worker.con.close()

            self.assertEqual(exit_code, 0)
            self.assertTrue(popen.call_args.kwargs["start_new_session"])


class CancelTests(unittest.TestCase):
    def test_cancel_terminates_process_group_for_running_job(self):
        import forge.commands.cancel as cancel

        with tempfile.TemporaryDirectory() as state_dir:
            db_path = os.path.join(state_dir, "queue.db")
            con = sql.connect(db_path)
            init_db(con)
            insert_job(con, job_id="job-1", status=JobStatus.RUNNING, pid=12345)
            con.close()

            def killpg(pid, sig):
                if sig == 0:
                    raise ProcessLookupError

            with mock.patch.object(cancel, "FORGE_PATH", state_dir), \
                 mock.patch.object(cancel.os, "killpg", side_effect=killpg) as mocked_killpg, \
                 mock.patch("builtins.print"):
                cancel.handle_cancel("job-1")

            self.assertEqual(
                mocked_killpg.mock_calls,
                [mock.call(12345, signal.SIGTERM), mock.call(12345, 0)],
            )

            con = sql.connect(db_path)
            job = get_job(con, "job-1")
            con.close()

            self.assertEqual(job.status, JobStatus.CANCELED)
            self.assertEqual(job.pid, 0)


if __name__ == "__main__":
    unittest.main()
