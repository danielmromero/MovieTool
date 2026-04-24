#!/usr/bin/env python3
from __future__ import annotations
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
import webbrowser
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"
STATE_FILE = DATA_DIR / "server_state.json"
LOG_FILE = DATA_DIR / "server.log"
PYTHON = sys.executable or "python3"


def read_state():
    if not STATE_FILE.exists():
        return None
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None


def is_healthy(url: str) -> bool:
    try:
        with urllib.request.urlopen(url.rstrip("/") + "/health", timeout=2) as resp:
            return resp.status == 200
    except Exception:
        return False


def start_server():
    DATA_DIR.mkdir(exist_ok=True)
    server_py = APP_DIR / "server.py"
    with LOG_FILE.open("a", encoding="utf-8") as log:
        subprocess.Popen(
            [PYTHON, str(server_py)],
            cwd=str(APP_DIR),
            stdout=log,
            stderr=log,
            start_new_session=True,
        )


def main():
    state = read_state()
    if state and state.get("url") and is_healthy(state["url"]):
        webbrowser.open(state["url"])
        return
    start_server()
    url = None
    for _ in range(80):
        time.sleep(0.25)
        state = read_state()
        if state and state.get("url"):
            url = state["url"]
            if is_healthy(url):
                webbrowser.open(url)
                return
    
    details = ''
    try:
        details = '\n\nLast log lines:\n' + ''.join(LOG_FILE.read_text(encoding='utf-8', errors='ignore').splitlines(True)[-12:])
    except Exception:
        pass
    raise SystemExit("Daniel's Movie Tool could not be started. Check data/server.log for details." + details)


if __name__ == "__main__":
    main()
