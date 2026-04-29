from pathlib import Path
import os
import sqlite3 as sql
import time

from forge import FORGE_PATH
from forge.db import Job, JobStatus, init_db


def handle_submit(filename):
    script_path = str(Path(filename).expanduser().resolve())
    submit_cwd = str(Path.cwd().resolve())

    os.makedirs(os.path.expanduser(FORGE_PATH), exist_ok=True)
    con = sql.connect(os.path.expanduser(f"{FORGE_PATH}/queue.db"))
    init_db(con)

    job = Job(
        script_path=script_path,
        submit_cwd=submit_cwd,
        status=JobStatus.QUEUED,
    )

    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO jobs (id, script_path, submit_cwd, status, created_at_ms)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            job.id,
            job.script_path,
            job.submit_cwd,
            int(job.status),
            int(time.time() * 1000),
        ),
    )
    con.commit()
    con.close()

    print(f"Job succesfully queued with UUID {job.id}")
