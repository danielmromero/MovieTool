#!/usr/bin/env python3
from __future__ import annotations
import json
import os
import signal
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"
STATE_FILE = DATA_DIR / "server_state.json"

if STATE_FILE.exists():
    try:
        state = json.loads(STATE_FILE.read_text(encoding='utf-8'))
        pid = int(state.get('pid'))
        os.kill(pid, signal.SIGTERM)
        print("Stopped Daniel's Movie Tool.")
    except Exception as exc:
        print(f"Could not stop Daniel's Movie Tool cleanly: {exc}")
else:
    print("Daniel's Movie Tool does not appear to be running.")
