import json
import os
import time

from forge import FORGE_PATH


def _job_log_path(job_id: str) -> str:
    return os.path.expanduser(f"{FORGE_PATH}/logs/{job_id}/out.log")


def _format_event(payload: dict) -> str:
    ts = payload.get("ts", "-")
    event = payload.get("event", "unknown_event")
    level = payload.get("level", "INFO")
    msg = payload.get("msg", "")

    if event == "job_output":
        stream = payload.get("stream", "stdout")
        line = payload.get("line", "")
        return f"[{ts}] [{stream}] {line}"

    return f"[{ts}] [{level}] [{event}] {msg}"


def _print_line(raw_line: str):
    text = raw_line.strip()
    if not text:
        return

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        print(text)
        return

    print(_format_event(payload))


def handle_logs(job_id: str, follow: bool = False):
    path = _job_log_path(job_id)

    if not os.path.exists(path):
        print(f"No log file found for job {job_id}")
        return

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            _print_line(line)

        if not follow:
            return

        while True:
            line = f.readline()
            if line:
                _print_line(line)
                continue
            time.sleep(0.5)
