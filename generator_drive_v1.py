# generator_sheetcells_logged.py
# Live Google Sheet → Colab notebooks in Drive + summary tab (code read from sheet cells)
# Adds refresh_date and refresh_time (Pacific Time) to the summary sheet.
# Existing functionality preserved:
#  - Services selected by presence of <service>_initial_db columns (ignores services_needed)
#  - Latest porting code per service read from "Translate_JSONs" (or env CODE_SHEET_NAME)
#  - Output Drive folder can be a folder ID or a subfolder under MyDrive/port_automation
#  - Output folder emptied before generation
#  - Notebook structure and logging unchanged except for new summary columns

from __future__ import annotations

import os
import sys
import re
import json
import pprint
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone

import nbformat
from nbformat.v4 import new_notebook, new_markdown_cell, new_code_cell

# --- Colab / Google APIs ---
from google.colab import auth, drive as gdrive_mount  # type: ignore
from googleapiclient.discovery import build           # type: ignore
from googleapiclient.http import MediaInMemoryUpload  # type: ignore

# =========================
# Config via environment
# =========================
SPREADSHEET_ID       = os.environ.get("SPREADSHEET_ID", "").strip()          # REQUIRED (tasks)
SOURCE_SHEET_NAME    = os.environ.get("SOURCE_SHEET_NAME", "").strip()       # optional (tasks tab)
SUMMARY_SHEET_NAME   = os.environ.get("SUMMARY_SHEET_NAME", "Generated_Colabs").strip()

# OUT_FOLDER_NAME may be a Drive FOLDER ID (preferred) or a subfolder NAME under MyDrive/port_automation
OUT_FOLDER_NAME      = os.environ.get("OUT_FOLDER_NAME", "generated_colabs").strip()

# Optional: if the code lives in a separate spreadsheet
CODE_SPREADSHEET_ID  = os.environ.get("CODE_SPREADSHEET_ID", "").strip()
CODE_SHEET_NAME      = os.environ.get("CODE_SHEET_NAME", "Translate_JSONs").strip()  # REQUIRED

# Drive paths
MYDRIVE_ROOT         = "/content/drive/MyDrive"
CODEBASE_FOLDER_NAME = "port_automation"               # codebase folder in Drive
CODEBASE_ROOT        = os.path.join(MYDRIVE_ROOT, CODEBASE_FOLDER_NAME)

# =========================
# Logging (stream live in Colab)
# =========================
os.environ["PYTHONUNBUFFERED"] = "1"
try:
    sys.stdout.reconfigure(line_buffering=True)  # py3.7+
except Exception:
    pass

def init_logging(level: str = "INFO") -> logging.Logger:
    lvl = getattr(logging, level.upper(), logging.INFO)

    # Remove handlers
    root = logging.getLogger()
    for h in root.handlers[:]:
        root.removeHandler(h)
    gen = logging.getLogger("generator")
    for h in gen.handlers[:]:
        gen.removeHandler(h)

    # stdout handler
    h = logging.StreamHandler(sys.stdout)
    h.setLevel(lvl)
    h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", "%Y-%m-%d %H:%M:%S"))
    root.setLevel(lvl)
    root.addHandler(h)

    gen.setLevel(lvl)
    gen.propagate = True
    return gen

log = init_logging("INFO")

# =========================
# Default DB paths (by API package)
# =========================
DEFAULT_DB_PATHS: Dict[str, str] = {
    "contacts":           "DBs/ContactsDefaultDB.json",
    "whatsapp":           "/content/DBs/WhatsAppDefaultDB.json",
    "google_calendar":    "/content/DBs/CalendarDefaultDB.json",
    "gmail":              "/content/DBs/GmailDefaultDB.json",
    "device_setting":     "/content/DBs/DeviceSettingDefaultDB.json",   # singular package
    "media_control":      "/content/DBs/MediaControlDefaultDB.json",
    "clock":              "/content/DBs/ClockDefaultDB.json",
    "generic_reminders":  "/content/DBs/GenericRemindersDefaultDB.json",
    "notes_and_lists":    "/content/DBs/NotesAndListsDefaultDB.json",
}

# =========================
# Tracker service → API package (+ implicit deps)
# =========================
SERVICE_SPECS: Dict[str, Dict[str, Any]] = {
    "whatsapp":         {"api": "whatsapp",           "requires": ["contacts"]},
    "contacts":         {"api": "contacts",           "requires": []},
    "calendar":         {"api": "google_calendar",    "requires": []},
    "gmail":            {"api": "gmail",              "requires": []},
    "device_settings":  {"api": "device_setting",     "requires": []},   # singular package
    "media_control":    {"api": "media_control",      "requires": []},
    "clock":            {"api": "clock",              "requires": []},
    "reminders":        {"api": "generic_reminders",  "requires": []},
    "notes":            {"api": "notes_and_lists",    "requires": []},
}

# =========================
# Porting specs (inject vars + call)
# json_vars: (sheet_column, notebook_var_name, inject_as_dict)
# =========================
PORTING_SPECS: Dict[str, Dict[str, Any]] = {
    "whatsapp": {
        "json_vars": [
            ("contacts_initial_db", "contacts_src_json",  False),
            ("whatsapp_initial_db", "whatsapp_src_json",  False),
        ],
        "pre_call_lines": [
            "port_contact_db = contacts_src_json",
            "port_whatsapp_db = whatsapp_src_json",
        ],
        "call": "port_db_whatsapp_and_contacts(port_contact_db, port_whatsapp_db)",
    },
    "calendar": {
        "json_vars":   [("calendar_initial_db", "port_calender_db", True)],
        "call":        "port_calendar_db(json.dumps(port_calender_db, ensure_ascii=False))",
    },
    "contacts": {
        "json_vars":   [("contacts_initial_db", "contacts_src_json", False)],
        "call":        "port_db_contacts(contacts_src_json)",
    },
    "gmail": {
        "json_vars":   [("gmail_initial_db", "gmail_src_json", False)],
        "pre_call_lines": ["port_gmail_db_key = gmail_src_json"],
        "call":        "port_gmail_db(port_gmail_db_key)",
    },
    "device_settings": {
        "json_vars":   [("device_settings_initial_db", "device_settings_src_json", False)],
        "call":        "port_device_setting_db(device_settings_src_json)",
    },
    "media_control": {
        "json_vars":   [("media_control_initial_db", "media_control_src_json", False)],
        "call":        "port_media_control_db(media_control_src_json)",
    },
    "clock": {
        # Keep as JSON string (porter uses json.loads internally)
        "json_vars":   [("clock_initial_db", "clock_src_json", False)],
        "call":        "port_clock_db(clock_src_json)",
    },
    "reminders": {
        "json_vars":   [("reminders_initial_db", "reminders_src_json", False)],
        "call":        "port_generic_reminder_db(reminders_src_json)",
    },
    "notes": {
        "json_vars":   [("notes_initial_db", "notes_src_json", False)],
        "call":        "port_notes_and_lists_initial_db(notes_src_json)",
    },
}

# First entry is the *primary* column used to decide if the service is selected
REQUIRED_INPUTS: Dict[str, List[str]] = {
    "whatsapp":        ["whatsapp_initial_db", "contacts_initial_db"],
    "contacts":        ["contacts_initial_db"],
    "calendar":        ["calendar_initial_db"],
    "gmail":           ["gmail_initial_db"],
    "device_settings": ["device_settings_initial_db"],
    "media_control":   ["media_control_initial_db"],
    "clock":           ["clock_initial_db"],
    "reminders":       ["reminders_initial_db"],
    "notes":           ["notes_initial_db"],
}

# =========================
# Helpers
# =========================
def mount_and_import_codebase():
    gdrive_mount.mount("/content/drive", force_remount=False)
    if not os.path.isdir(CODEBASE_ROOT):
        raise RuntimeError(
            f"Codebase folder not found at {CODEBASE_ROOT}. "
            f"Place your repo under MyDrive/{CODEBASE_FOLDER_NAME}/"
        )
    if CODEBASE_ROOT not in sys.path:
        sys.path.append(CODEBASE_ROOT)

    # Import static cells from your codebase (used in the generated notebooks)
    from static_code.setup_code_cell import setup_cell
    from static_code.pipinstall_cell import pipinstall_cell
    return setup_cell, pipinstall_cell

def auth_services():
    auth.authenticate_user()
    drive = build("drive", "v3")
    sheets = build("sheets", "v4")
    return drive, sheets

def normalize_service_token(tok: str) -> str:
    t = re.sub(r"[/&]", " ", str(tok).strip().lower())
    t = re.sub(r"\s+", " ", t)
    synonyms = {
        "google calendar": "calendar",
        "calender": "calendar",
        "google mail": "gmail",
        "email": "gmail",
        "e-mail": "gmail",
        "media control": "media_control",
        "device settings": "device_settings",
        "whatsapp message": "whatsapp",
        "whatsapp messages": "whatsapp",
        "message": "whatsapp",
        "messages": "whatsapp",
        "reminder": "reminders",
        "generic reminders": "reminders",
        "notes and lists": "notes",
        "notes_and_lists": "notes",
    }
    return synonyms.get(t, t)

def parse_initial_db(cell_value: Optional[str]) -> Dict[str, Any]:
    """Parse strict JSON from sheet cell (so true/false/null are valid)."""
    if cell_value is None:
        return {}
    s = str(cell_value).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return {}
    return json.loads(s)

def py_literal(obj: Any) -> str:
    return pprint.pformat(obj, width=100, sort_dicts=False)

def reescape_newlines_inside_string_literals(src: str) -> str:
    """Re-escape real newlines in quoted strings → '\\n' (keeps print('...') intact)."""
    if not src:
        return ""
    s = src.replace("\r\n", "\n").replace("\r", "\n")
    out = []
    i, n = 0, len(s)
    in_str = False
    triple = False
    quote = ""
    escape = False
    in_comment = False
    while i < n:
        ch = s[i]
        if in_comment:
            out.append(ch)
            if ch == "\n":
                in_comment = False
            i += 1
            continue
        if in_str:
            if escape:
                out.append(ch)
                escape = False
            elif ch == "\\":
                out.append(ch)
                escape = True
            elif triple:
                if ch == quote and i + 2 < n and s[i + 1] == quote and s[i + 2] == quote:
                    out += [ch, quote, quote]
                    i += 3
                    in_str = False
                    triple = False
                else:
                    out.append("\\n" if ch == "\n" else ch)
                    i += 1
                continue
            else:
                if ch == "\n":
                    out.append("\\n")
                elif ch == quote:
                    out.append(ch)
                    in_str = False
                else:
                    out.append(ch)
            i += 1
            continue
        if ch == "#":
            in_comment = True
            out.append(ch)
            i += 1
            continue
        if ch in ("'", '"'):
            if i + 2 < n and s[i + 1] == ch and s[i + 2] == ch:
                out += [ch, ch, ch]
                i += 3
                in_str = True
                triple = True
                quote = ch
            else:
                out.append(ch)
                i += 1
                in_str = True
                triple = False
                quote = ch
            continue
        out.append(ch)
        i += 1
    return "".join(out)

# ---------- Google Drive helpers

def find_root_folder_id(drive, name: str) -> str:
    q = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and 'root' in parents and trashed=false"
    res = drive.files().list(q=q, fields="files(id,name)").execute().get("files", [])
    if not res:
        raise RuntimeError(f"Folder '{name}' not found in My Drive root.")
    return res[0]["id"]

def ensure_subfolder(drive, parent_id: str, name: str) -> str:
    q = (
        f"'{parent_id}' in parents and name='{name}' and "
        f"mimeType='application/vnd.google-apps.folder' and trashed=false"
    )
    res = drive.files().list(q=q, fields="files(id,name)").execute().get("files", [])
    if res:
        return res[0]["id"]
    meta = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
    folder = drive.files().create(body=meta, fields="id").execute()
    return folder["id"]

def resolve_output_folder_id(drive, out_hint: str, codebase_folder_name: str) -> str:
    """Accept OUT_FOLDER_NAME as folder ID or as a name under MyDrive/<codebase_folder_name>."""
    if out_hint:
        # Try as ID
        try:
            meta = drive.files().get(fileId=out_hint, fields="id,name,mimeType,trashed").execute()
            if meta and meta.get("mimeType") == "application/vnd.google-apps.folder" and not meta.get("trashed", False):
                log.info("Using provided Drive folder ID: %s (%s)", meta["id"], meta["name"])
                return meta["id"]
        except Exception:
            pass  # not an ID → treat as a name
        base_id = find_root_folder_id(drive, codebase_folder_name)
        sub_id = ensure_subfolder(drive, base_id, out_hint)
        log.info("Using/created subfolder '%s' under MyDrive/%s", out_hint, codebase_folder_name)
        return sub_id
    # Default
    base_id = find_root_folder_id(drive, codebase_folder_name)
    return ensure_subfolder(drive, base_id, "generated_colabs")

def empty_drive_folder(drive, folder_id: str) -> int:
    """Remove all items in the given Drive folder."""
    log.info("Emptying output folder (id=%s) before generation …", folder_id)
    removed = 0
    page_token = None
    while True:
        resp = drive.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id, name, mimeType)",
            pageToken=page_token
        ).execute()
        files = resp.get("files", [])
        if not files:
            break
        for f in files:
            fid = f["id"]; fname = f.get("name",""); mt = f.get("mimeType","")
            try:
                drive.files().delete(fileId=fid).execute()
                removed += 1
                log.info("  - removed: %s (%s)", fname, mt)
            except Exception as e:
                log.warning("  - could not remove %s (%s): %s", fname, fid, e)
        page_token = resp.get("nextPageToken", None)
        if not page_token:
            break
    log.info("Output folder emptied: %d item(s) removed.", removed)
    return removed

def upload_notebook_to_drive(drive, folder_id: str, filename: str, nb: nbformat.NotebookNode) -> Tuple[str, str]:
    data = nbformat.writes(nb).encode("utf-8")
    media = MediaInMemoryUpload(data, mimetype="application/vnd.google.colaboratory", resumable=False)
    meta = {"name": filename, "mimeType": "application/vnd.google.colaboratory", "parents": [folder_id]}
    file = drive.files().create(body=meta, media_body=media, fields="id,webViewLink").execute()
    file_id = file["id"]
    colab_url = f"https://colab.research.google.com/drive/{file_id}"
    return file_id, colab_url

# ---------- Google Sheets helpers (robust rows)

def get_first_sheet_title(sheets, spreadsheet_id: str) -> str:
    meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    return meta["sheets"][0]["properties"]["title"]

def read_sheet_as_dicts(sheets, spreadsheet_id: str, sheet_name: str) -> Tuple[List[str], List[Dict[str, str]]]:
    """Robustly read a sheet; handles ragged rows without IndexError."""
    rng = f"'{sheet_name}'"
    resp = sheets.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    values = resp.get("values", [])
    if not values:
        return [], []
    headers = [h.strip() for h in values[0]]
    rows: List[Dict[str, str]] = []
    for r in values[1:]:
        row_dict: Dict[str, str] = {}
        for i in range(len(headers)):
            row_dict[headers[i]] = r[i] if i < len(r) else ""
        rows.append(row_dict)
    return headers, rows

# ---------- Service selection by *_initial_db columns

def services_from_initial_db_columns(row: Dict[str, str]) -> List[str]:
    """
    For each service, check its PRIMARY initial-db column (first entry of REQUIRED_INPUTS[service]).
    If the cell is non-empty, select the service. Dependencies are added later.
    """
    selected: List[str] = []
    for svc, cols in REQUIRED_INPUTS.items():
        if not cols:
            continue
        primary = cols[0]
        if str(row.get(primary, "")).strip():
            selected.append(svc)
    return selected

# ---------- Notebook builders

def build_metadata_cell(task_id: str, api_modules: List[str]):
    md = [
        f"**Sample ID**: {task_id}\n\n",
        "**Query**:\n\n",
        "**DB Type**: Base Case\n\n",
        "**Case Description**:\n\n",
        "**Global/Context Variables:**\n\n",
        "**APIs:**\n",
    ]
    md += [f"- {a}\n" for a in api_modules]
    md.append("\n**Databases:**")
    return new_markdown_cell("".join(md))

def build_warnings_cell(issues: Dict[str, Any]):
    msgs = []
    if issues["unknown_services"]:
        msgs.append(f"- Unknown/unsupported services: `{', '.join(issues['unknown_services'])}`")
    if issues["missing_inputs"]:
        msgs.append(f"- Missing required inputs: `{', '.join(issues['missing_inputs'])}`")
    if issues["json_errors"]:
        msgs.append("- JSON parse errors:\n  - " + "\n  - ".join(f"`{k}` → {v}" for k, v in issues["json_errors"].items()))
    return new_markdown_cell("### Warnings detected for this row\n\n" + "\n".join(msgs)) if msgs else None

def build_setup_cells(setup_cell: str, pipinstall_cell: str):
    setup_src = reescape_newlines_inside_string_literals(setup_cell).strip() + "\n"
    pip_src = reescape_newlines_inside_string_literals(pipinstall_cell).strip() + "\n"
    return [
        new_markdown_cell("## Download relevant files"),
        new_code_cell(setup_src),
        new_markdown_cell("## Install Dependencies and Clone Repositories"),
        new_code_cell(pip_src),
    ]

def build_import_and_port_cell(
    api_modules: List[str],
    expanded_services: List[str],
    row: Dict[str, str],
    code_map: Dict[str, str],
    meta_map: Dict[str, Tuple[str, str]],
):
    L: List[str] = []
    L.append("# Imports")
    for m in api_modules:
        L.append(f"import {m}")
    if "notes_and_lists" in api_modules:
        L.append("from notes_and_lists.SimulationEngine.utils import update_title_index, update_content_index")
        L.append("from typing import Dict, Any")
        L.append("from datetime import timezone")
    L += ["import json, uuid", "from datetime import datetime", ""]
    L.append("# Load default DBs")
    for api in api_modules:
        if api in DEFAULT_DB_PATHS:
            L.append(f'{api}.SimulationEngine.db.load_state("{DEFAULT_DB_PATHS[api]}")')
    L.append("")

    calls: List[str] = []

    for svc in expanded_services:
        spec = PORTING_SPECS.get(svc)
        if not spec:
            L += [f"# (No porting spec defined for '{svc}'; skipping)", ""]
            continue

        # Inject inputs
        for col, var, as_dict in spec.get("json_vars", []):
            try:
                d = parse_initial_db(row.get(col))
            except Exception:
                d = {}
            if as_dict:
                L += [f"# {var} from {col} (dict)", f"{var} = {py_literal(d)}", ""]
            else:
                L += [f"# {var} from {col} (JSON string)", f"{var} = json.dumps({py_literal(d)}, ensure_ascii=False)", ""]

        # Paste live code from sheet with meta note
        code_str = code_map.get(svc, "")
        if code_str:
            code_str = reescape_newlines_inside_string_literals(code_str).strip()
            date_upd, resp = meta_map.get(svc, ("", ""))
            L += [
                f"# ==== Porting code for service: {svc} (from live sheet) ====",
                f"# Using latest porting code for '{svc}' which was updated on {date_upd} by {resp}",
                code_str,
                "",
            ]
            for ln in spec.get("pre_call_lines", []):
                L.append(ln)
            if spec.get("pre_call_lines"):
                L.append("")
            calls.append(spec["call"])
        else:
            L += [f"# (No code found in code sheet for service '{svc}')", ""]

    if calls:
        L += ["# Execute porting"] + calls
    return new_code_cell("\n".join(L) + "\n")

def build_empty_block(title: str):
    return [new_markdown_cell(f"# {title}"), new_code_cell("")]

# ---------- Preflight & parsing

def preflight_row(row: Dict[str, str]) -> Dict[str, Any]:
    """
    Select services by *_initial_db presence; add dependencies; validate inputs.
    """
    issues = {"unknown_services": [], "missing_inputs": [], "json_errors": {}}

    services = services_from_initial_db_columns(row)

    expanded = list(services)
    for s in services:
        spec = SERVICE_SPECS.get(s)
        if spec:
            for req in spec["requires"]:
                if req not in expanded:
                    expanded.append(req)

    issues["unknown_services"] = [s for s in expanded if s not in SERVICE_SPECS]

    need = sorted({c for s in expanded for c in REQUIRED_INPUTS.get(s, [])})
    for col in need:
        v = row.get(col, "")
        if not str(v).strip():
            issues["missing_inputs"].append(col)
        else:
            try:
                json.loads(str(v))
            except Exception as e:
                issues["json_errors"][col] = str(e)
    return {"services": services, "expanded": expanded, "issues": issues}

# ---------- Code sheet: latest-per-service logic with logging

def _find_header(headers: List[str], candidates: List[str]) -> Optional[str]:
    """Find a header by exact (case-insensitive) or contains matching."""
    hnorm = [h.strip().lower() for h in headers]
    # exact first
    for cand in candidates:
        cand_l = cand.strip().lower()
        for i, h in enumerate(hnorm):
            if h == cand_l:
                return headers[i]
    # contains fallback
    for cand in candidates:
        cand_l = cand.strip().lower()
        for i, h in enumerate(hnorm):
            if cand_l in h:
                return headers[i]
    return None

def _parse_any_date(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    fmts = [
        "%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y",
        "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%m/%d/%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S",
        "%b %d, %Y", "%d %b %Y", "%b %d %Y", "%Y.%m.%d",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def build_service_code_map_with_logs(sheets) -> Tuple[Dict[str, str], Dict[str, Tuple[str, str]]]:
    """
    Returns:
      code_map: service -> code string
      meta_map: service -> (date_updated_display, responsible_person_display)
    Logs the chosen latest entry per service.
    """
    sheet_id_for_code = CODE_SPREADSHEET_ID or SPREADSHEET_ID
    if not CODE_SHEET_NAME:
        raise RuntimeError("CODE_SHEET_NAME is required to read service code from the sheet.")

    headers, rows = read_sheet_as_dicts(sheets, sheet_id_for_code, CODE_SHEET_NAME)
    if not rows:
        raise RuntimeError(f"No rows found in code sheet '{CODE_SHEET_NAME}'.")

    # Columns
    svc_col  = _find_header(headers, ["service_name", "service", "api", "services"])
    code_col = _find_header(headers, ["function_to_translate_json", "code", "porting_code", "port_code"])
    if not svc_col or not code_col:
        raise RuntimeError("Could not find required columns 'service_name' and/or 'function_to_translate_json' in code sheet.")

    date_col = _find_header(headers, ["translate_jsons(date_updated)", "date_updated", "last_updated", "updated_at", "modified_at", "date"])
    resp_col = _find_header(headers, ["responsible person", "responsible", "owner", "author", "updated_by"])

    # Group by normalized service; choose latest by date
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for r in rows:
        raw_svc = (r.get(svc_col) or "").strip()
        if not raw_svc:
            continue
        svc_norm = normalize_service_token(raw_svc)
        grouped.setdefault(svc_norm, []).append(r)

    code_map: Dict[str, str] = {}
    meta_map: Dict[str, Tuple[str, str]] = {}

    for svc, items in grouped.items():
        best_idx = -1
        best_dt: Optional[datetime] = None
        for i, it in enumerate(items):
            dt_str = (it.get(date_col) if date_col else "") or ""
            dt = _parse_any_date(dt_str) if dt_str else None
            if best_dt is None or (dt and dt > best_dt):
                best_dt = dt
                best_idx = i

        chosen = items[best_idx] if best_idx >= 0 else items[0]
        code_str = chosen.get(code_col, "") or ""
        if not code_str:
            continue

        code_map[svc] = code_str

        chosen_date = (chosen.get(date_col) or "N/A") if date_col else "N/A"
        chosen_resp = (chosen.get(resp_col) or "N/A") if resp_col else "N/A"
        meta_map[svc] = (chosen_date, chosen_resp)

        log.info(
            "Using latest porting code for '%s' which was updated on %s by %s",
            svc,
            chosen_date,
            chosen_resp,
        )

    return code_map, meta_map

# ---------- Notebook generation

def build_import_and_port_cell(
    api_modules: List[str],
    expanded_services: List[str],
    row: Dict[str, str],
    code_map: Dict[str, str],
    meta_map: Dict[str, Tuple[str, str]],
):
    L: List[str] = []
    L.append("# Imports")
    for m in api_modules:
        L.append(f"import {m}")
    if "notes_and_lists" in api_modules:
        L.append("from notes_and_lists.SimulationEngine.utils import update_title_index, update_content_index")
        L.append("from typing import Dict, Any")
        L.append("from datetime import timezone")
    L += ["import json, uuid", "from datetime import datetime", ""]
    L.append("# Load default DBs")
    for api in api_modules:
        if api in DEFAULT_DB_PATHS:
            L.append(f'{api}.SimulationEngine.db.load_state("{DEFAULT_DB_PATHS[api]}")')
    L.append("")

    calls: List[str] = []

    for svc in expanded_services:
        spec = PORTING_SPECS.get(svc)
        if not spec:
            L += [f"# (No porting spec defined for '{svc}'; skipping)", ""]
            continue

        # Inject inputs
        for col, var, as_dict in spec.get("json_vars", []):
            try:
                d = parse_initial_db(row.get(col))
            except Exception:
                d = {}
            if as_dict:
                L += [f"# {var} from {col} (dict)", f"{var} = {py_literal(d)}", ""]
            else:
                L += [f"# {var} from {col} (JSON string)", f"{var} = json.dumps({py_literal(d)}, ensure_ascii=False)", ""]

        # Paste live code with meta line
        code_str = code_map.get(svc, "")
        if code_str:
            code_str = reescape_newlines_inside_string_literals(code_str).strip()
            date_upd, resp = meta_map.get(svc, ("", ""))
            L += [
                f"# ==== Porting code for service: {svc} (from live sheet) ====",
                f"# Using latest porting code for '{svc}' which was updated on {date_upd} by {resp}",
                code_str,
                "",
            ]
            for ln in spec.get("pre_call_lines", []):
                L.append(ln)
            if spec.get("pre_call_lines"):
                L.append("")
            calls.append(spec["call"])
        else:
            L += [f"# (No code found in code sheet for service '{svc}')", ""]

    if calls:
        L += ["# Execute porting"] + calls
    return new_code_cell("\n".join(L) + "\n")

def build_empty_block(title: str):
    return [new_markdown_cell(f"# {title}"), new_code_cell("")]

def generate_notebook_for_row(
    row: Dict[str, str],
    idx: int,
    setup_cell: str,
    pipinstall_cell: str,
    code_map: Dict[str, str],
    meta_map: Dict[str, Tuple[str, str]],
) -> Tuple[nbformat.NotebookNode, Dict[str, Any]]:
    pre = preflight_row(row)
    services = pre["services"]
    expanded = pre["expanded"]
    issues = pre["issues"]

    # API modules list (respect implicit deps order)
    api_modules: List[str] = []
    for s in services:
        spec = SERVICE_SPECS.get(s)
        if not spec:
            continue
        api = spec["api"]
        if api not in api_modules:
            api_modules.append(api)
        for req in spec.get("requires", []):
            ra = SERVICE_SPECS[req]["api"]
            if ra not in api_modules:
                api_modules.append(ra)

    nb = new_notebook()
    task_id = (row.get("task_id") or f"row-{idx}").strip() or f"row-{idx}"
    nb.cells.append(build_metadata_cell(task_id, api_modules))
    w = build_warnings_cell(issues)
    if w:
        nb.cells.append(w)
    nb.cells.append(new_markdown_cell("# Set Up"))
    nb.cells.extend(build_setup_cells(setup_cell, pipinstall_cell))
    nb.cells.append(new_markdown_cell("## Import APIs and initiate DBs"))
    nb.cells.append(build_import_and_port_cell(api_modules, expanded, row, code_map, meta_map))
    nb.cells.extend(build_empty_block("Initial Assertion"))
    nb.cells.extend(build_empty_block("Action"))
    nb.cells.extend(build_empty_block("Final Assertion"))
    nb.metadata["colab"] = {"provenance": []}
    nb.metadata["language_info"] = {"name": "python"}
    return nb, issues

# ---------- Summary writer with PST timestamp

def _now_pacific() -> Tuple[str, str]:
    """
    Return (date_str, time_str) in America/Los_Angeles.
    Uses zoneinfo if present; falls back to naive UTC->PST/PDT guess if unavailable.
    """
    try:
        from zoneinfo import ZoneInfo  # py>=3.9
        tz = ZoneInfo("America/Los_Angeles")
        dt = datetime.now(timezone.utc).astimezone(tz)
    except Exception:
        # Fallback (no DST awareness): still returns something sensible
        dt = datetime.now()
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")

def upsert_summary_sheet_3col(sheets, spreadsheet_id: str, sheet_name: str, rows_3col: List[List[str]]):
    """
    Kept the same function name/signature for backward compatibility,
    but now writes FIVE columns:
      task_id, services_required, colab_url, refresh_date, refresh_time
    where refresh_* are the same for all rows and computed in Pacific Time.
    """
    # ensure sheet exists (or clear)
    meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    existing = {sh["properties"]["title"] for sh in meta.get("sheets", [])}
    if sheet_name not in existing:
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]},
        ).execute()
        log.info("Created summary sheet tab: %s", sheet_name)
    else:
        sheets.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=f"'{sheet_name}'"
        ).execute()
        log.info("Cleared existing rows in summary sheet tab: %s", sheet_name)

    # Compute Pacific timestamp once for this refresh
    refresh_date, refresh_time = _now_pacific()
    log.info("Summary refresh timestamp (Pacific): %s %s", refresh_date, refresh_time)

    # Extend rows with refresh columns
    rows_5col = [r + [refresh_date, refresh_time] for r in rows_3col]

    headers = ["task_id", "services_required", "colab_url", "refresh_date", "refresh_time"]
    body = {"range": f"'{sheet_name}'!A1", "majorDimension": "ROWS", "values": [headers] + rows_5col}
    sheets.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!A1",
        valueInputOption="RAW",
        body=body,
    ).execute()

    # Set row height to ~18px
    meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id = None
    for sh in meta["sheets"]:
        if sh["properties"]["title"] == sheet_name:
            sheet_id = sh["properties"]["sheetId"]
            break
    if sheet_id is not None:
        row_count = len(rows_5col) + 1
        requests = [
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 0, "endIndex": row_count},
                    "properties": {"pixelSize": 18},
                    "fields": "pixelSize",
                }
            }
        ]
        sheets.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute()
        log.info("Adjusted row heights on summary tab to 18px for %d rows", row_count)
    else:
        log.warning("Could not find sheetId for summary tab; row height not adjusted.")

# ---------- Main flow

def main():
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID is required. Set os.environ['SPREADSHEET_ID'] before running.")
    if not CODE_SHEET_NAME:
        raise RuntimeError("CODE_SHEET_NAME is required. Set os.environ['CODE_SHEET_NAME'].")

    # Mount & import your codebase (static cells)
    setup_cell, pipinstall_cell = mount_and_import_codebase()
    log.info("Mounted Drive and imported static setup cells from %s", CODEBASE_ROOT)

    # Auth to Google APIs
    drive, sheets = auth_services()
    log.info("Authenticated to Google Drive & Sheets APIs.")

    # Resolve output folder (ID or name) and empty it
    out_folder_id = resolve_output_folder_id(drive, OUT_FOLDER_NAME, CODEBASE_FOLDER_NAME)
    log.info("Output notebooks Drive folder id: %s", out_folder_id)
    empty_drive_folder(drive, out_folder_id)

    # Resolve source task sheet
    src_name = SOURCE_SHEET_NAME or get_first_sheet_title(sheets, SPREADSHEET_ID)
    log.info("Reading task rows from sheet '%s' (spreadsheet id: %s)", src_name, SPREADSHEET_ID)

    # Read tasks (robust)
    task_headers, task_rows = read_sheet_as_dicts(sheets, SPREADSHEET_ID, src_name)
    if not task_rows:
        log.warning("No data found in source task sheet.")
        return
    log.info("Loaded %d task rows.", len(task_rows))

    # Read code map from live sheet (select latest by date), with logs
    code_map, meta_map = build_service_code_map_with_logs(sheets)
    log.info("Prepared porting code for %d services.", len(code_map))

    rows_3col: List[List[str]] = []
    problems: List[Tuple[str, Dict[str, Any]]] = []

    for i, row in enumerate(task_rows, start=1):
        task_id = (row.get("task_id") or f"row-{i}").strip() or f"row-{i}"

        selected_services = services_from_initial_db_columns(row)
        log.info("---- Generating notebook for task_id=%s ----", task_id)
        log.info("Selected services (from *_initial_db): %s", " | ".join(selected_services) if selected_services else "(none)")

        services_required_for_summary = row.get("services_needed", "").strip()

        nb, issues = generate_notebook_for_row(row, i, setup_cell, pipinstall_cell, code_map, meta_map)
        fname = f"Gemini_Apps_ID_Data_Port_{task_id}.ipynb"
        _, colab_url = upload_notebook_to_drive(drive, out_folder_id, fname, nb)
        log.info("Uploaded: %s", fname)
        log.info("Colab URL: %s", colab_url)

        rows_3col.append([task_id, services_required_for_summary, colab_url])

        if issues["unknown_services"] or issues["missing_inputs"] or issues["json_errors"]:
            problems.append((task_id, issues))
            if issues["unknown_services"]:
                log.warning("Unknown services for %s: %s", task_id, ", ".join(issues["unknown_services"]))
            if issues["missing_inputs"]:
                log.warning("Missing inputs for %s: %s", task_id, ", ".join(issues["missing_inputs"]))
            if issues["json_errors"]:
                for col, err in issues["json_errors"].items():
                    log.warning("JSON error in %s for %s: %s", col, task_id, err)

    # Write summary tab back to spreadsheet (now 5 columns including refresh_date/time)
    upsert_summary_sheet_3col(sheets, SPREADSHEET_ID, SUMMARY_SHEET_NAME, rows_3col)
    log.info("Wrote summary tab '%s' with %d rows.", SUMMARY_SHEET_NAME, len(rows_3col))

    if problems:
        log.info("Completed with warnings on %d row(s). See log above.", len(problems))
    else:
        log.info("Completed successfully with no warnings.")
