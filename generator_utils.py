# generator_utils.py
from __future__ import annotations

import os, sys, re, json, pprint, logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone

import nbformat
from nbformat.v4 import new_notebook, new_markdown_cell, new_code_cell

# colab + google apis (present in Colab)
from google.colab import auth, drive as gdrive_mount  # type: ignore
from googleapiclient.discovery import build           # type: ignore
from googleapiclient.http import MediaInMemoryUpload  # type: ignore

# ----------------------------
# Logging (stdout, live in Colab)
# ----------------------------
os.environ["PYTHONUNBUFFERED"] = "1"
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

def init_logging(level: str = "INFO") -> logging.Logger:
    lvl = getattr(logging, level.upper(), logging.INFO)

    root = logging.getLogger()
    for h in root.handlers[:]:
        root.removeHandler(h)
    lg = logging.getLogger("generator")
    for h in lg.handlers[:]:
        lg.removeHandler(h)

    h = logging.StreamHandler(sys.stdout)
    h.setLevel(lvl)
    h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", "%Y-%m-%d %H:%M:%S"))
    root.setLevel(lvl)
    root.addHandler(h)

    lg.setLevel(lvl)
    lg.propagate = True
    return lg

log = init_logging("INFO")

# ----------------------------
# Codebase & defaults
# ----------------------------
MYDRIVE_ROOT         = "/content/drive/MyDrive"
CODEBASE_FOLDER_NAME = "port_automation"
CODEBASE_ROOT        = os.path.join(MYDRIVE_ROOT, CODEBASE_FOLDER_NAME)

DEFAULT_DB_PATHS: Dict[str, str] = {
    "contacts":           "DBs/ContactsDefaultDB.json",
    "whatsapp":           "/content/DBs/WhatsAppDefaultDB.json",
    "google_calendar":    "/content/DBs/CalendarDefaultDB.json",
    "gmail":              "/content/DBs/GmailDefaultDB.json",
    "device_setting":     "/content/DBs/DeviceSettingDefaultDB.json",
    "media_control":      "/content/DBs/MediaControlDefaultDB.json",
    "clock":              "/content/DBs/ClockDefaultDB.json",
    "generic_reminders":  "/content/DBs/GenericRemindersDefaultDB.json",
    "notes_and_lists":    "/content/DBs/NotesAndListsDefaultDB.json",
    "device_actions":    "/content/DBs/DeviceActionsDefaultDB.json",
}

SERVICE_SPECS: Dict[str, Dict[str, Any]] = {
    "whatsapp":         {"api": "whatsapp",           "requires": ["contacts"]},
    "contacts":         {"api": "contacts",           "requires": []},
    "calendar":         {"api": "google_calendar",    "requires": []},
    "gmail":            {"api": "gmail",              "requires": []},
    "device_settings":  {"api": "device_setting",     "requires": []},
    "media_control":    {"api": "media_control",      "requires": []},
    "clock":            {"api": "clock",              "requires": []},
    "reminders":        {"api": "generic_reminders",  "requires": []},
    "notes":            {"api": "notes_and_lists",    "requires": []},
    "device_actions":   {"api": "device_actions",     "requires": []},
}

# json_vars: (sheet_column, notebook_var_name, inject_as_dict)
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
        # keep as JSON string (porter uses json.loads internally)
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
     "device_actions": {
        "json_vars":   [("device_actions_initial_db", "device_actions_src_json", False)],
        "call":        "port_device_actions_db(device_actions_initial_db)",
    },
}

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
    "device_actions":  ["device_actions_initial_db"],
}

# ----------------------------
# Small helpers
# ----------------------------
def mount_and_import_codebase():
    gdrive_mount.mount("/content/drive", force_remount=False)
    if not os.path.isdir(CODEBASE_ROOT):
        raise RuntimeError(
            f"Codebase folder not found at {CODEBASE_ROOT}. "
            f"Place your repo under MyDrive/{CODEBASE_FOLDER_NAME}/"
        )
    if CODEBASE_ROOT not in sys.path:
        sys.path.append(CODEBASE_ROOT)
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
        "google calendar": "calendar", "calender": "calendar",
        "google mail": "gmail", "email": "gmail", "e-mail": "gmail",
        "media control": "media_control",
        "device settings": "device_settings",
        "whatsapp message": "whatsapp", "whatsapp messages": "whatsapp",
        "message": "whatsapp", "messages": "whatsapp",
        "reminder": "reminders", "generic reminders": "reminders",
        "notes and lists": "notes", "notes_and_lists": "notes",
        "device actions": "device_actions",
    }
    return synonyms.get(t, t)

def parse_initial_db(cell_value: Optional[str]) -> Dict[str, Any]:
    if cell_value is None: return {}
    s = str(cell_value).strip()
    if not s or s.lower() in {"nan","none","null"}: return {}
    return json.loads(s)

def py_literal(obj: Any) -> str:
    return pprint.pformat(obj, width=100, sort_dicts=False)

def reescape_newlines_inside_string_literals(src: str) -> str:
    if not src: return ""
    s = src.replace("\r\n","\n").replace("\r","\n")
    out=[]; i=0; n=len(s); in_str=False; triple=False; quote=''; escape=False; in_comment=False
    while i<n:
        ch=s[i]
        if in_comment:
            out.append(ch); 
            if ch=="\n": in_comment=False
            i+=1; continue
        if in_str:
            if escape: out.append(ch); escape=False
            elif ch=="\\": out.append(ch); escape=True
            elif triple:
                if ch==quote and i+2<n and s[i+1]==quote and s[i+2]==quote:
                    out+=[ch,quote,quote]; i+=3; in_str=triple=False
                else:
                    out.append("\\n" if ch=="\n" else ch); i+=1
                continue
            else:
                if ch=="\n": out.append("\\n")
                elif ch==quote: out.append(ch); in_str=False
                else: out.append(ch)
            i+=1; continue
        if ch=="#":
            in_comment=True; out.append(ch); i+=1; continue
        if ch in ("'",'"'):
            if i+2<n and s[i+1]==ch and s[i+2]==ch:
                out+=[ch,ch,ch]; i+=3; in_str=True; triple=True; quote=ch
            else:
                out.append(ch); i+=1; in_str=True; triple=False; quote=ch
            continue
        out.append(ch); i+=1
    return "".join(out)

# ----- Drive helpers
def find_root_folder_id(drive, name: str) -> str:
    q = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and 'root' in parents and trashed=false"
    res = drive.files().list(q=q, fields="files(id,name)").execute().get("files", [])
    if not res:
        raise RuntimeError(f"Folder '{name}' not found in My Drive root.")
    return res[0]["id"]

def ensure_subfolder(drive, parent_id: str, name: str) -> str:
    q = f"'{parent_id}' in parents and name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    res = drive.files().list(q=q, fields="files(id,name)").execute().get("files", [])
    if res:
        return res[0]["id"]
    meta = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
    folder = drive.files().create(body=meta, fields="id").execute()
    return folder["id"]

def resolve_output_folder_id(drive, out_hint: str, codebase_folder_name: str) -> str:
    if out_hint:
        try:
            meta = drive.files().get(fileId=out_hint, fields="id,name,mimeType,trashed").execute()
            if meta and meta.get("mimeType") == "application/vnd.google-apps.folder" and not meta.get("trashed", False):
                log.info("Using provided Drive folder ID: %s (%s)", meta["id"], meta["name"])
                return meta["id"]
        except Exception:
            pass
        base_id = find_root_folder_id(drive, codebase_folder_name)
        sub_id  = ensure_subfolder(drive, base_id, out_hint)
        log.info("Using/created subfolder '%s' under MyDrive/%s", out_hint, codebase_folder_name)
        return sub_id
    base_id = find_root_folder_id(drive, codebase_folder_name)
    return ensure_subfolder(drive, base_id, "generated_colabs")

def empty_drive_folder(drive, folder_id: str) -> int:
    log.info("Emptying output folder (id=%s) before generation …", folder_id)
    removed = 0; page_token = None
    while True:
        resp = drive.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id, name, mimeType)", pageToken=page_token
        ).execute()
        files = resp.get("files", [])
        if not files: break
        for f in files:
            try:
                drive.files().delete(fileId=f["id"]).execute()
                removed += 1
                log.info("  - removed: %s", f.get("name",""))
            except Exception as e:
                log.warning("  - could not remove %s (%s)", f.get("name",""), e)
        page_token = resp.get("nextPageToken", None)
        if not page_token: break
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

# ----- Sheets helpers
def get_first_sheet_title(sheets, spreadsheet_id: str) -> str:
    meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    return meta["sheets"][0]["properties"]["title"]

def read_sheet_as_dicts(sheets, spreadsheet_id: str, sheet_name: str) -> Tuple[List[str], List[Dict[str, str]]]:
    rng = f"'{sheet_name}'"
    resp = sheets.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    values = resp.get("values", [])
    if not values:
        return [], []
    headers = [h.strip() for h in values[0]]
    rows: List[Dict[str, str]] = []
    for r in values[1:]:
        rows.append({headers[i]: (r[i] if i < len(r) else "") for i in range(len(headers))})
    return headers, rows

# ----- Service selection
def services_from_initial_db_columns(row: Dict[str, str]) -> List[str]:
    selected: List[str] = []
    for svc, cols in REQUIRED_INPUTS.items():
        if not cols: continue
        primary = cols[0]
        if str(row.get(primary, "")).strip():
            selected.append(svc)
    return selected

# ----- Code sheet (latest per service) + logs
def _find_header(headers: List[str], candidates: List[str]) -> Optional[str]:
    hnorm = [h.strip().lower() for h in headers]
    for cand in candidates:
        c = cand.strip().lower()
        for i,h in enumerate(hnorm):
            if h == c: return headers[i]
    for cand in candidates:
        c = cand.strip().lower()
        for i,h in enumerate(hnorm):
            if c in h: return headers[i]
    return None

def _parse_any_date(s: str) -> Optional[datetime]:
    if not s: return None
    s = s.strip()
    fmts = [
        "%Y-%m-%d","%Y/%m/%d","%d-%m-%Y","%m/%d/%Y","%d/%m/%Y",
        "%Y-%m-%d %H:%M:%S","%Y/%m/%d %H:%M:%S","%m/%d/%Y %H:%M:%S","%d/%m/%Y %H:%M:%S",
        "%b %d, %Y","%d %b %Y","%b %d %Y","%Y.%m.%d",
    ]
    for fmt in fmts:
        try: return datetime.strptime(s, fmt)
        except Exception: pass
    try: return datetime.fromisoformat(s.replace("Z","+00:00"))
    except Exception: return None

def build_service_code_map_with_logs(sheets, spreadsheet_id: str, code_sheet_name: str) -> Tuple[Dict[str, str], Dict[str, Tuple[str, str]]]:
    headers, rows = read_sheet_as_dicts(sheets, spreadsheet_id, code_sheet_name)
    if not rows:
        raise RuntimeError(f"No rows found in code sheet '{code_sheet_name}'.")

    svc_col  = _find_header(headers, ["service_name","service","api","services"])
    code_col = _find_header(headers, ["function_to_translate_json","code","porting_code","port_code"])
    if not svc_col or not code_col:
        raise RuntimeError("Missing 'service_name' and/or 'function_to_translate_json' in code sheet.")

    date_col = _find_header(headers, ["translate_jsons(date_updated)","date_updated","last_updated","updated_at","modified_at","date"])
    resp_col = _find_header(headers, ["responsible person","responsible","owner","author","updated_by"])

    grouped: Dict[str, List[Dict[str,str]]] = {}
    for r in rows:
        raw = (r.get(svc_col) or "").strip()
        if not raw: continue
        svc_norm = normalize_service_token(raw)
        grouped.setdefault(svc_norm, []).append(r)

    code_map: Dict[str,str] = {}
    meta_map: Dict[str,Tuple[str,str]] = {}

    for svc, items in grouped.items():
        best_idx=-1; best_dt=None
        for i,it in enumerate(items):
            ds = (it.get(date_col) if date_col else "") or ""
            dt = _parse_any_date(ds) if ds else None
            if best_dt is None or (dt and dt>best_dt):
                best_dt=dt; best_idx=i
        chosen = items[best_idx] if best_idx>=0 else items[0]
        code_str = chosen.get(code_col, "") or ""
        if not code_str: continue
        code_map[svc]=code_str
        d = (chosen.get(date_col) or "N/A") if date_col else "N/A"
        p = (chosen.get(resp_col) or "N/A") if resp_col else "N/A"
        meta_map[svc]=(d,p)
        log.info("Using latest porting code for '%s' which was updated on %s by %s", svc, d, p)

    return code_map, meta_map

# ----- Notebook builders (shared)
def build_setup_cells(setup_cell: str, pipinstall_cell: str):
    setup_src = reescape_newlines_inside_string_literals(setup_cell).strip() + "\n"
    pip_src   = reescape_newlines_inside_string_literals(pipinstall_cell).strip() + "\n"
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
    L += ["import os, json, uuid", "from datetime import datetime", ""]

    # USER_LOCATION env injection
    user_location = (row.get("user_location") or "").strip()
    L.append("# User location from sheet (environment variable for downstream code)")
    L.append(f'os.environ["USER_LOCATION"] = {json.dumps(user_location)}')
    L.append("")

    # Load defaults for selected APIs
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

        # inject initial DB variables
        for col, var, as_dict in spec.get("json_vars", []):
            try:
                d = parse_initial_db(row.get(col))
            except Exception:
                d = {}
            if as_dict:
                L += [f"# {var} from {col} (dict)", f"{var} = {py_literal(d)}", ""]
            else:
                L += [f"# {var} from {col} (JSON string)", f"{var} = json.dumps({py_literal(d)}, ensure_ascii=False)", ""]

        # live porting code from sheet
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

def build_warnings_cell(issues: Dict[str, Any]):
    msgs=[]
    if issues["unknown_services"]:
        msgs.append(f"- Unknown/unsupported services: `{', '.join(issues['unknown_services'])}`")
    if issues["missing_inputs"]:
        msgs.append(f"- Missing required inputs: `{', '.join(issues['missing_inputs'])}`")
    if issues["json_errors"]:
        msgs.append("- JSON parse errors:\n  - " + "\n  - ".join(f"`{k}` → {v}" for k,v in issues["json_errors"].items()))
    return new_markdown_cell("### Warnings detected for this row\n\n" + "\n".join(msgs)) if msgs else None

def build_empty_block(title: str):
    return [new_markdown_cell(f"# {title}"), new_code_cell("")]

# PST timestamp helpers + summary writers
def _now_pacific() -> Tuple[str, str]:
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("America/Los_Angeles")
        dt = datetime.now(timezone.utc).astimezone(tz)
    except Exception:
        dt = datetime.now()
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")

def upsert_summary_sheet(
    sheets,
    spreadsheet_id: str,
    sheet_name: str,
    headers: List[str],
    rows: List[List[str]],
    set_row_px: int = 18,
):
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

    body = {"range": f"'{sheet_name}'!A1", "majorDimension": "ROWS", "values": [headers] + rows}
    sheets.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!A1",
        valueInputOption="RAW",
        body=body,
    ).execute()

    meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id = None
    for sh in meta["sheets"]:
        if sh["properties"]["title"] == sheet_name:
            sheet_id = sh["properties"]["sheetId"]; break
    if sheet_id is not None:
        row_count = len(rows) + 1
        requests = [{
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 0, "endIndex": row_count},
                "properties": {"pixelSize": set_row_px},
                "fields": "pixelSize",
            }
        }]
        sheets.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute()
        log.info("Adjusted row heights on summary tab to %dpx for %d rows", set_row_px, row_count)
