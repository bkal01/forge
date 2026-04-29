import os
import sqlite3 as sql
from datetime import datetime
from pathlib import Path

from forge import FORGE_PATH
from forge.db import JobStatus, get_jobs_by_status, init_db


def handle_list() -> None:
    db_path = os.path.expanduser(f"{FORGE_PATH}/queue.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    con = sql.connect(db_path)
    init_db(con)

    jobs = get_jobs_by_status(con, [JobStatus.RUNNING, JobStatus.QUEUED])
    con.close()

    rows = [
        (
            datetime.fromtimestamp(
                (job.started_at_ms if job.status == JobStatus.RUNNING else job.created_at_ms) / 1000
            ).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z"),
            job.id,
            Path(job.script_path).name,
            "running" if job.status == JobStatus.RUNNING else "queued",
        )
        for job in jobs
    ]

    headers = ("timestamp", "uuid", "script", "status")
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    print("  ".join(cell.ljust(widths[i]) for i, cell in enumerate(headers)))
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print("  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)))
