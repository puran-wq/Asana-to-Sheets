#!/usr/bin/env python3
"""
Task Tracker → Asana  |  Web App
Flask backend: serves the UI and exposes a /sync endpoint.
"""

import json
import logging
import os
import threading
from datetime import datetime
from pathlib import Path

import gspread
import asana
from flask import Flask, jsonify, render_template, request, Response, stream_with_context
from google.oauth2.service_account import Credentials

# ── App setup ─────────────────────────────────────────────────────────────────

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SPREADSHEET_ID  = "1p49dQjCTLMe1ZHjL09NOtzDAcEDRUDSczVj_NMLb7KQ"
WORKSHEET_NAME  = "Sheet1"
STATE_FILE      = ".sync_state.json"

# ── Server-side credentials (set these as environment variables) ──────────────
# Team members never see or handle any of these.
ASANA_TOKEN     = os.environ["ASANA_ACCESS_TOKEN"]       # Your service account token
ASANA_PROJECT   = os.environ["ASANA_PROJECT_GID"]
ASANA_WORKSPACE = os.environ["ASANA_WORKSPACE_GID"]
# Google credentials loaded from an env var (JSON string) or a local file
_creds_raw = os.environ.get("GOOGLE_CREDENTIALS_JSON")
GOOGLE_CREDS: dict = json.loads(_creds_raw) if _creds_raw else json.loads(
    Path(os.environ.get("GOOGLE_CREDENTIALS_FILE", "credentials.json")).read_text()
)

# In-memory sync log for streaming to the UI
_sync_log: list[dict] = []
_sync_lock = threading.Lock()
_sync_running = False

# ── Helpers (reused from sheets_to_asana.py) ──────────────────────────────────

DATE_FORMATS = [
    "%B %d, %Y at %I:%M %p %Z",
    "%B %d, %Y at %I:%M %p",
    "%B %d, %Y",
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%d/%m/%Y",
]

def parse_date(value: str) -> str | None:
    value = value.strip()
    parts = value.rsplit(" ", 1)
    stripped = parts[0] if len(parts) == 2 and parts[1].isupper() and len(parts[1]) <= 4 else value
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(stripped, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def get_sheet_rows(credentials_json: dict = None) -> list[dict]:
    credentials_json = credentials_json or GOOGLE_CREDS
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_info(credentials_json, scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)
    rows = sheet.get_all_records()
    return [r for r in rows if any(str(v).strip() for v in r.values())]


def build_task_body(row: dict, project_gid: str, workspace_gid: str) -> dict:
    task_name    = row.get("Task Name:", "").strip()
    priority     = row.get("Priority:", "").strip()
    assigned_to  = row.get("Assigned To:", "").strip()
    assigned_by  = row.get("Assigned By:", "").strip()
    date_str     = row.get("Date Assigned:", "").strip()
    email_output = row.get("Email Output", "").strip()

    name = task_name or f"Task from Sheet – {datetime.now().strftime('%Y-%m-%d %H:%M')}"

    notes_parts = []
    if email_output:
        notes_parts.append(f"Email context:\n{email_output}")
    if assigned_by:
        notes_parts.append(f"Assigned by: {assigned_by}")
    if priority:
        notes_parts.append(f"Priority: {priority}")
    notes = "\n\n".join(notes_parts)

    body: dict = {
        "name":      name,
        "notes":     notes,
        "projects":  [project_gid],
        "workspace": workspace_gid,
    }

    if assigned_to and "@" in assigned_to:
        body["assignee"] = assigned_to
    elif assigned_to:
        body["notes"] += f"\n\nAssigned to: {assigned_to}"

    if date_str:
        parsed = parse_date(date_str)
        if parsed:
            body["due_on"] = parsed

    return body


def load_state() -> dict:
    if Path(STATE_FILE).exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"synced_row_count": 0}


def save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def emit(msg: str, level: str = "info"):
    entry = {"time": datetime.now().strftime("%H:%M:%S"), "msg": msg, "level": level}
    with _sync_lock:
        _sync_log.append(entry)
    log.info(msg)

# ── Sync worker (runs in background thread) ───────────────────────────────────

def run_sync():
    global _sync_running
    try:
        emit("Connecting to Google Sheets…")
        rows = get_sheet_rows()
        state = load_state()
        synced_so_far = state["synced_row_count"]
        new_rows = rows[synced_so_far:]

        if not new_rows:
            emit(f"No new rows found (sheet has {len(rows)} total rows, all synced).", "info")
            return

        emit(f"Found {len(new_rows)} new row(s) to sync.")

        configuration = asana.Configuration()
        configuration.access_token = ASANA_TOKEN
        tasks_api = asana.TasksApi(asana.ApiClient(configuration))

        success = 0
        for i, row in enumerate(new_rows, start=synced_so_far + 1):
            task_name = row.get("Task Name:", "Untitled").strip() or "Untitled"
            try:
                body = build_task_body(row, ASANA_PROJECT, ASANA_WORKSPACE)
                result = tasks_api.create_task({"data": body}, {})
                emit(f"✓ Row {i} → \"{task_name}\" (task {result['gid']})", "success")
                success += 1
            except Exception as exc:
                emit(f"✗ Row {i} → \"{task_name}\" failed: {exc}", "error")

        state["synced_row_count"] = synced_so_far + success
        state["last_sync"] = datetime.utcnow().isoformat()
        save_state(state)
        emit(f"Sync complete — {success}/{len(new_rows)} tasks created.", "success")

    except Exception as exc:
        emit(f"Fatal error: {exc}", "error")
    finally:
        _sync_running = False

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    state = load_state()
    return render_template("index.html", synced_count=state.get("synced_row_count", 0),
                           last_sync=state.get("last_sync", None))


@app.route("/sync", methods=["POST"])
def sync():
    global _sync_running, _sync_log

    if _sync_running:
        return jsonify({"error": "A sync is already running."}), 409

    with _sync_lock:
        _sync_log = []
        _sync_running = True

    thread = threading.Thread(target=run_sync, daemon=True)
    thread.start()
    return jsonify({"status": "started"})


@app.route("/log")
def log_stream():
    """Server-sent events — streams log entries to the browser."""
    after = int(request.args.get("after", 0))

    def generate():
        last_sent = after
        while True:
            with _sync_lock:
                entries = _sync_log[last_sent:]
            for entry in entries:
                yield f"data: {json.dumps(entry)}\n\n"
                last_sent += 1
            if not _sync_running and last_sent >= len(_sync_log):
                yield "data: {\"done\": true}\n\n"
                break
            import time; time.sleep(0.3)

    return Response(stream_with_context(generate()), mimetype="text/event-stream")


@app.route("/state")
def state():
    return jsonify(load_state())


@app.route("/reset", methods=["POST"])
def reset():
    if Path(STATE_FILE).exists():
        Path(STATE_FILE).unlink()
    return jsonify({"status": "reset"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)