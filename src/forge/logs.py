import json
import os
import threading
from datetime import datetime, timezone

from forge import FORGE_PATH


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class ForgeLogger:
    def __init__(self, run_id: str):
        self.run_id = run_id
        self._lock = threading.Lock()

        self.state_dir = os.path.expanduser(FORGE_PATH)
        self.logs_dir = os.path.join(self.state_dir, "logs")
        self.system_log_path = os.path.join(self.state_dir, "worker.log")

        os.makedirs(self.logs_dir, exist_ok=True)

    def system(self, level: str, event: str, msg: str, job_id=None, **fields):
        self._write(
            self.system_log_path,
            level=level,
            event=event,
            component="worker",
            job_id=job_id,
            msg=msg,
            **fields,
        )

    def for_job(self, job_id: str):
        return JobLogger(self, job_id)

    def _write(self, path: str, level: str, event: str, component: str, job_id, msg: str, **fields):
        payload = {
            "ts": utc_now_iso(),
            "level": level,
            "event": event,
            "component": component,
            "job_id": job_id,
            "run_id": self.run_id,
            "msg": msg,
        }
        payload.update(fields)

        line = json.dumps(payload, separators=(",", ":"))
        with self._lock:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
                f.flush()


class JobLogger:
    def __init__(self, logger: ForgeLogger, job_id: str):
        self._logger = logger
        self.job_id = job_id
        job_log_dir = os.path.join(self._logger.logs_dir, job_id)
        os.makedirs(job_log_dir, exist_ok=True)
        self.path = os.path.join(job_log_dir, "out.log")

    def worker(self, level: str, event: str, msg: str, **fields):
        self._logger._write(
            self.path,
            level=level,
            event=event,
            component="worker",
            job_id=self.job_id,
            msg=msg,
            **fields,
        )

    def output(self, stream: str, line: str):
        self._logger._write(
            self.path,
            level="INFO",
            event="job_output",
            component="job_runner",
            job_id=self.job_id,
            msg=stream,
            stream=stream,
            line=line,
        )
