from __future__ import annotations
from dateutil import parser
import os, sys, re, json, ast, time, pprint, logging, traceback
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import nbformat
from nbformat.v4 import new_notebook, new_markdown_cell, new_code_cell

# --- Colab / Google APIs ---
from google.colab import auth, drive as gdrive_mount  # type: ignore
from googleapiclient.discovery import build           # type: ignore
from googleapiclient.http import MediaInMemoryUpload  # type: ignore
from googleapiclient.errors import HttpError          # type: ignore

# =========================
# Config via environment
# =========================
SPREADSHEET_ID       = os.environ.get("SPREADSHEET_ID", "").strip()
SOURCE_SHEET_NAME    = os.environ.get("SOURCE_SHEET_NAME", "Template Colab").strip()
SOURCE_WORKING_SHEET_NAME = os.environ.get("SOURCE_WORKING_SHEET_NAME", "Working_Sheet").strip()

CODE_SPREADSHEET_ID  = "1nY7dC2pn4dQcBdRH3Ar5o1d0rv9UOOW_kXRamzN8GCM" # optional
CODE_SHEET_NAME      = os.environ.get("CODE_SHEET_NAME", "Translate_JSONs").strip()

SUMMARY_SHEET_NAME_WORKING_AUTOMATION = os.environ.get(
    "SUMMARY_SHEET_NAME_WORKING_AUTOMATION_FA_VAL", "Working_Sheet_Generated_Colabs"
).strip()

WS_OUT_FOLDER_NAME   = os.environ.get("WS_OUT_FOLDER_NAME_FA_VAL", "generated_colabs_ws").strip()
MAX_WORKERS          = int(os.environ.get("MAX_WORKERS", "6"))

# Final-assertion column names (env-controlled)
FINAL_ASSERTION_COL_NAME = os.environ.get("FINAL_ASSERTION_COL_NAME", "final_assertion_code").strip()
MODIFIED_FINAL_ASSERTION_COL_NAME = os.environ.get("MODIFIED_FINAL_ASSERTION_COL_NAME", "modified_final_assertion_code").strip()

# Drive paths
MYDRIVE_ROOT         = "/content/drive/MyDrive"
CODEBASE_FOLDER_NAME = "port_automation"
CODEBASE_ROOT        = os.path.join(MYDRIVE_ROOT, CODEBASE_FOLDER_NAME)



GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
DEFAULT_GEMINI_MODEL_NAME = os.environ.get('DEFAULT_GEMINI_MODEL_NAME',"gemini-2.5-pro-preview-03-25")
LIVE_API_URL = os.environ.get('LIVE_API_URL')

# =========================
# Logging
# =========================
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
    lg = logging.getLogger("generator_ws_parallel")
    for h in lg.handlers[:]:
        lg.removeHandler(h)
    h = logging.StreamHandler(sys.stdout)
    h.setLevel(lvl)
    h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", "%Y-%m-%d %H:%M:%S"))
    root.setLevel(lvl); root.addHandler(h)
    lg.setLevel(lvl); lg.propagate = True
    return lg

log = init_logging("INFO")

# =========================
# Default DB paths
# =========================
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
}

# =========================
# Service specs
# =========================
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
}

# =========================
# Porting specs (initial stage)
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

# For FINAL DB injection we override the primary var to the service's own var
SELF_VAR_BY_SERVICE: Dict[str, Tuple[str, bool]] = {
    "whatsapp":        ("whatsapp_src_json", False),
    "contacts":        ("contacts_src_json", False),
    "calendar":        ("port_calender_db",  True),
    "gmail":           ("gmail_src_json",    False),
    "device_settings": ("device_settings_src_json", False),
    "media_control":   ("media_control_src_json",   False),
    "clock":           ("clock_src_json",    False),
    "reminders":       ("reminders_src_json",False),
    "notes":           ("notes_src_json",    False),
}

# Primary initial DB column per service
PRIMARY_INITIAL_DB_COL: Dict[str, str] = {
    "contacts":        "contacts_initial_db",
    "calendar":        "calendar_initial_db",
    "gmail":           "gmail_initial_db",
    "whatsapp":        "whatsapp_initial_db",
    "device_settings": "device_settings_initial_db",
    "media_control":   "media_control_initial_db",
    "clock":           "clock_initial_db",
    "reminders":       "reminders_initial_db",
    "notes":           "notes_initial_db",
}

# =========================
# Utils
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
    from static_code.setup_code_cell import setup_cell
    from static_code.pipinstall_cell import pipinstall_cell
    return setup_cell, pipinstall_cell

def auth_services():
    auth.authenticate_user()
    drive = build("drive", "v3")
    sheets = build("sheets", "v4")
    return drive, sheets

def normalize_service_token(tok: str) -> str:
    t = re.sub(r"[/&]", " ", str(tok).strip().lower()); t = re.sub(r"\s+", " ", t)
    synonyms = {
        "google calendar": "calendar", "calender": "calendar",
        "google mail": "gmail", "email": "gmail", "e-mail": "gmail",
        "media control": "media_control",
        "device settings": "device_settings",
        "whatsapp message": "whatsapp", "whatsapp messages": "whatsapp",
        "message": "whatsapp", "messages": "whatsapp",
        "reminder": "reminders", "generic reminders": "reminders",
        "notes and lists": "notes", "notes_and_lists": "notes",
    }
    return synonyms.get(t, t)

def split_services(cell: Optional[str]) -> List[str]:
    if not cell: return []
    tokens = re.split(r"[|,]", cell)
    out, seen = [], set()
    for tok in tokens:
        name = normalize_service_token(tok)
        if name and name not in seen:
            out.append(name); seen.add(name)
    return out

def parse_initial_db(cell_value: Optional[str]) -> Dict[str, Any]:
    if cell_value is None: return {}
    s = str(cell_value).strip()
    if not s or s.lower() in {"nan","none","null"}: return {}
    return json.loads(s)

def parse_json_best_effort(cell_value: Optional[str]) -> Dict[str, Any]:
    if cell_value is None: return {}
    s = str(cell_value).strip()
    if not s or s.lower() in {"nan","none","null"}: return {}
    try:
        return json.loads(s)
    except Exception:
        pass
    try:
        val = ast.literal_eval(s)
        if isinstance(val, (dict, list)):
            return val
    except Exception:
        pass
    return {}

def py_literal(obj: Any) -> str:
    return pprint.pformat(obj, width=100, sort_dicts=False)

def reescape_newlines_inside_string_literals(src: str) -> str:
    if not src: return ""
    s = src.replace("\r\n","\n").replace("\r","\n")
    out=[]; i=0; n=len(s); in_str=False; triple=False; quote=''; escape=False; in_comment=False
    while i<n:
        ch=s[i]
        if in_comment:
            out.append(ch)
            if ch=="\n": in_comment=False
            i+=1; continue
        if in_str:
            if escape: out.append(ch); escape=False
            elif ch=="\\": out.append(ch); escape=True
            elif triple:
                if ch==quote and i+2<n and s[i+1]==quote and s[i+2]==quote:
                    out+=[ch,quote,quote]; i+=3; in_str=False; triple=False
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

# ---------- Drive helpers
def find_root_folder_id(drive, name: str) -> str:
    q = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and 'root' in parents and trashed=false"
    res = drive.files().list(q=q, fields="files(id,name)").execute().get("files", [])
    if not res: raise RuntimeError(f"Folder '{name}' not found in My Drive root.")
    return res[0]["id"]

def ensure_subfolder(drive, parent_id: str, name: str) -> str:
    q = (f"'{parent_id}' in parents and name='{name}' and "
         f"mimeType='application/vnd.google-apps.folder' and trashed=false")
    res = drive.files().list(q=q, fields="files(id,name)").execute().get("files", [])
    if res: return res[0]["id"]
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
        sub_id = ensure_subfolder(drive, base_id, out_hint)
        log.info("Using/created subfolder '%s' under MyDrive/%s", out_hint, codebase_folder_name)
        return sub_id
    base_id = find_root_folder_id(drive, codebase_folder_name)
    return ensure_subfolder(drive, base_id, "generated_colabs_ws")

def empty_drive_folder(drive, folder_id: str) -> int:
    log.info("Emptying output folder (id=%s) before generation …", folder_id)
    removed=0; page_token=None
    while True:
        resp = drive.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id, name, mimeType)",
            pageToken=page_token
        ).execute()
        files = resp.get("files", [])
        if not files: break
        for f in files:
            fid=f["id"]; fname=f.get("name",""); mt=f.get("mimeType","")
            try:
                drive.files().delete(fileId=fid).execute()
                removed+=1; log.info("  - removed: %s (%s)", fname, mt)
            except Exception as e:
                log.warning("  - could not remove %s (%s): %s", fname, fid, e)
        page_token = resp.get("nextPageToken")
        if not page_token: break
    log.info("Output folder emptied: %d item(s) removed.", removed)
    return removed

def upload_notebook_to_drive_with_retries(folder_id: str, filename: str, nb: nbformat.NotebookNode,
                                          max_retries: int = 5, base_delay: float = 1.0) -> Tuple[str, str]:
    attempt = 0
    last_err: Optional[Exception] = None
    while attempt <= max_retries:
        try:
            drive = build("drive", "v3")  # fresh per thread
            data = nbformat.writes(nb).encode("utf-8")
            media = MediaInMemoryUpload(data, mimetype="application/vnd.google.colaboratory", resumable=False)
            meta  = {"name": filename, "mimeType": "application/vnd.google.colaboratory", "parents": [folder_id]}
            file  = drive.files().create(body=meta, media_body=media, fields="id,webViewLink").execute()
            file_id = file["id"]
            colab_url = f"https://colab.research.google.com/drive/{file_id}"
            return file_id, colab_url
        except HttpError as e:
            last_err = e
            status = getattr(e.resp, "status", None)
            if status in (403, 429, 500, 502, 503, 504):
                sleep_s = base_delay * (2 ** attempt) + (0.1 * attempt)
                log.warning("Upload retry %d for %s due to HTTP %s; sleeping %.1fs",
                            attempt + 1, filename, status, sleep_s)
                time.sleep(sleep_s)
                attempt += 1
                continue
            raise
        except Exception as e:
            last_err = e
            sleep_s = base_delay * (2 ** attempt) + (0.1 * attempt)
            log.warning("Upload retry %d for %s after error: %s; sleeping %.1fs",
                        attempt + 1, filename, e, sleep_s)
            time.sleep(sleep_s); attempt += 1
    if last_err: raise last_err
    raise RuntimeError("Unknown upload failure.")

# ---------- Sheets helpers
def get_first_sheet_title(sheets, spreadsheet_id: str) -> str:
    meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    return meta["sheets"][0]["properties"]["title"]

def read_sheet_as_dicts(sheets, spreadsheet_id: str, sheet_name: str) -> Tuple[List[str], List[Dict[str, str]]]:
    rng = f"'{sheet_name}'"
    resp = sheets.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    values = resp.get("values", [])
    if not values: return [], []
    headers = [h.strip() for h in values[0]]
    rows: List[Dict[str, str]] = []
    for r in values[1:]:
        row_dict = {headers[i]: (r[i] if i < len(r) else "") for i in range(len(headers))}
        rows.append(row_dict)
    return headers, rows

# ---------- DATA-DRIVEN initial services
def compute_initial_services_from_template_row(template_row: Dict[str, str]) -> List[str]:
    selected: List[str] = []
    for svc, col in PRIMARY_INITIAL_DB_COL.items():
        if str(template_row.get(col, "")).strip():
            selected.append(svc)
    if "whatsapp" in selected and "contacts" in selected:
        selected.remove("contacts")
    return selected

def api_modules_for_services(services: List[str]) -> List[str]:
    """Return API modules for the given services + their dependencies, deduped in order."""
    modules: List[str] = []
    for s in services:
        spec = SERVICE_SPECS.get(s)
        if not spec: continue
        api = spec["api"]
        if api not in modules:
            modules.append(api)
        for req in spec.get("requires", []):
            req_api = SERVICE_SPECS[req]["api"]
            if req_api not in modules:
                modules.append(req_api)
    return modules

# ---------- Code sheet readers
def _find_header(headers: List[str], candidates: List[str]) -> Optional[str]:
    hnorm = [h.strip().lower() for h in headers]
    for cand in candidates:
        cand_l = cand.strip().lower()
        for i, h in enumerate(hnorm):
            if h == cand_l:
                return headers[i]
    for cand in candidates:
        cand_l = cand.strip().lower()
        for i, h in enumerate(hnorm):
            if cand_l in h:
                return headers[i]
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

def build_service_code_map_with_logs(
    sheets,
    spreadsheet_id: str,
    code_sheet_name: str,
    code_col_candidates: List[str],
    date_col_candidates: List[str] = None,
    resp_col_candidates: List[str] = None,
) -> Tuple[Dict[str, str], Dict[str, Tuple[str, str]]]:
    headers, rows = read_sheet_as_dicts(sheets, spreadsheet_id, code_sheet_name)
    if not rows:
        raise RuntimeError(f"No rows found in code sheet '{code_sheet_name}'.")
    svc_col  = _find_header(headers, ["service_name","service","api","services"])
    code_col = _find_header(headers, code_col_candidates or ["function_to_translate_json"])
    if not svc_col or not code_col:
        raise RuntimeError(f"Missing service/code columns in code sheet '{code_sheet_name}'.")
    date_col = _find_header(headers, (date_col_candidates or
                                      ["translate_jsons(date_updated)","date_updated","last_updated","updated_at","modified_at","date"]))
    resp_col = _find_header(headers, (resp_col_candidates or
                                      ["responsible person","responsible","owner","author","updated_by"]))

    grouped: Dict[str, List[Dict[str, str]]] = {}
    for r in rows:
        raw_svc = (r.get(svc_col) or "").strip()
        if not raw_svc: continue
        svc_norm = normalize_service_token(raw_svc)
        grouped.setdefault(svc_norm, []).append(r)

    code_map: Dict[str, str] = {}
    meta_map: Dict[str, Tuple[str, str]] = {}
    for svc, items in grouped.items():
        best_idx = -1; best_dt: Optional[datetime] = None
        for i, it in enumerate(items):
            dt_str = (it.get(date_col) if date_col else "") or ""
            dt = _parse_any_date(dt_str) if dt_str else None
            if best_dt is None or (dt and dt > best_dt):
                best_dt = dt; best_idx = i
        chosen = items[best_idx] if best_idx >= 0 else items[0]
        code_str = chosen.get(code_col, "") or ""
        if not code_str: continue
        code_map[svc] = code_str
        chosen_date = (chosen.get(date_col) or "N/A") if date_col else "N/A"
        chosen_resp = (chosen.get(resp_col) or "N/A") if resp_col else "N/A"
        meta_map[svc] = (chosen_date, chosen_resp)
        log.info("Using latest porting code for '%s' (%s) updated on %s by %s",
                 svc, code_col, chosen_date, chosen_resp)
    return code_map, meta_map

# ---------- Summary writer (PST)
def _now_pacific() -> Tuple[str, str]:
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("America/Los_Angeles")
        dt = datetime.now(timezone.utc).astimezone(tz)
    except Exception:
        dt = datetime.now()
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")

def upsert_summary_sheet_ws(sheets, spreadsheet_id: str, sheet_name: str, rows: List[List[str]]):
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

    refresh_date, refresh_time = _now_pacific()
    rows6 = [r + [refresh_date, refresh_time] for r in rows]

    headers = ["sample_id", "task_id", "services_required", "colab_url", "refresh_date", "refresh_time"]
    body = {"range": f"'{sheet_name}'!A1", "majorDimension": "ROWS", "values": [headers] + rows6}
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
        row_count = len(rows6) + 1
        req = [{
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 0, "endIndex": row_count},
                "properties": {"pixelSize": 18},
                "fields": "pixelSize",
            }
        }]
        sheets.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": req}).execute()

# ---------- Notebook builders
def build_metadata_cell(
    sample_id: str,
    query_text: str,
    api_modules: List[str],
    query_date: str,
    uploaded_file_url: str = "",
    public_tools:list[str]
):
    # Parse query_date similar to Freezegun logic: validate with dateutil.parser; if invalid, omit.
    dt_value = ""
    if query_date:
        try:
            _ = parser.parse(query_date)
            dt_value = query_date.strip()
        except Exception:
            dt_value = ""

    md = [
        f"**Sample ID**: {sample_id}\n\n",
        f"**Query**: {query_text or ''}\n\n",
        "**DB Type**: Base Case\n\n",
        "**Case Description**:\n\n",
    ]

    if uploaded_file_url:
        md.append("```\n<additional_data>\n")
        md.append(f'  <current_uploaded_file src="{uploaded_file_url}" />\n')
        md.append("</additional_data>\n```\n\n")

    md.extend([
        "**Global/Context Variables:**\n\n\n",
        "**Datetime Context Variables:**\n",
    ])

    if dt_value:
        md.append(f"- {dt_value}\n\n")
    else:
        md.append("\n")  # keep structure even if empty

    md.append("**APIs:**\n")
    md += [f"- {a}\n" for a in api_modules]
    if public_tools:
        md += [f"- {p}\n" for p in public_tools]
    md.append("\n**Databases:**")

    return new_markdown_cell("".join(md))


def build_warnings_cell(issues: Dict[str, Any]):
    msgs=[]
    if issues["unknown_services"]: msgs.append(f"- Unknown/unsupported services: `{', '.join(issues['unknown_services'])}`")
    if issues["missing_inputs"]:   msgs.append(f"- Missing required inputs: `{', '.join(issues['missing_inputs'])}`")
    if issues["json_errors"]:      msgs.append("- JSON parse errors:\n  - " + "\n  - ".join(f"`{k}` → {v}" for k,v in issues["json_errors"].items()))
    return new_markdown_cell("### Warnings detected for this row\n\n" + "\n".join(msgs)) if msgs else None

def build_setup_cells(setup_cell: str, pipinstall_cell: str):
    setup_src = reescape_newlines_inside_string_literals(setup_cell).strip() + "\n"
    pip_src   = reescape_newlines_inside_string_literals(pipinstall_cell).strip() + "\n"
    return [
        new_markdown_cell("## Download relevant files"),
        new_code_cell(setup_src),
        new_markdown_cell("## Install Dependencies and Clone Repositories"),
        new_code_cell(pip_src),
    ]

def build_import_and_port_cell_ws(
    api_modules: List[str],
    services_for_code: List[str],         # data-driven list from initial DB columns
    template_row: Dict[str, str],
    code_map_initial: Dict[str, str],
    meta_map_initial: Dict[str, Tuple[str, str]],
    user_location_value: str,
    query_date: str,
    public_tools:List[str]
):
    L: List[str] = []
    L.append("# Imports")
    L = add_gemini_keys(L,public_tools
    # Add Freezegun as first import
    # L = add_freezegun_block(L,query_date)
    for m in api_modules: L.append(f"import {m}")
    if "notes_and_lists" in api_modules:
        L.append("from notes_and_lists.SimulationEngine.utils import update_title_index, update_content_index")
        L.append("from typing import Dict, Any")
        L.append("from datetime import timezone")
    L += ["import json, uuid", "from datetime import datetime", "import os", ""]
    # USER_LOCATION injection (force single line, escape)
    raw_loc = str(user_location_value or "")
    single_line = " ".join(raw_loc.split())
    user_loc_val = single_line.replace("\\", "\\\\").replace('\"', '\\"')
    L.append('# User location from Working Sheet')
    L.append(f'os.environ["USER_LOCATION"] = "{user_loc_val}"')
    L.append("")
    # Load default DBs
    L.append("# Load default DBs")
    for api in api_modules:
        if api in DEFAULT_DB_PATHS:
            L.append(f'{api}.SimulationEngine.db.load_state("{DEFAULT_DB_PATHS[api]}")')
    L.append("")

    calls: List[str] = []

    for svc in services_for_code:
        spec = PORTING_SPECS.get(svc)
        if not spec:
            L += [f"# (No porting spec defined for '{svc}'; skipping)", ""]
            continue

        # Inject inputs — from TEMPLATE row (initial DBs)
        for col, var, as_dict in spec.get("json_vars", []):
            try:
                d = parse_initial_db(template_row.get(col))
            except Exception:
                d = {}
            if as_dict:
                L += [f"# {var} from Template Colab → {col} (dict)", f"{var} = {py_literal(d)}", ""]
            else:
                L += [f"# {var} from Template Colab → {col} (JSON string)", f"{var} = json.dumps({py_literal(d)}, ensure_ascii=False)", ""]

        # Paste live code with meta line (initial)
        code_str = code_map_initial.get(svc, "")
        if code_str:
            code_str = reescape_newlines_inside_string_literals(code_str).strip()
            date_upd, resp = meta_map_initial.get(svc, ("", ""))
            L += [
                f"# ==== Porting code for service: {svc} (from live sheet: function_to_translate_json) ====",
                f"# Using latest porting code for '{svc}' which was updated on {date_upd} by {resp}",
                code_str,
                "",
            ]
            for ln in spec.get("pre_call_lines", []): L.append(ln)
            if spec.get("pre_call_lines"): L.append("")
            calls.append(spec["call"])
        else:
            L += [f"# (No initial code found in code sheet for service '{svc}')", ""]

    if calls:
        L += ["# Execute initial porting"] + calls
    return new_code_cell("\n".join(L) + "\n")

def final_db_col_for_service(svc: str) -> str:
    """Working Sheet FINAL DB column — '<service>_final_db'."""
    return f"{svc}_final_db"

def build_action_final_dbs_cell_ws(
    final_services: List[str],
    working_row: Dict[str, str],
    code_map_final: Dict[str, str],
    meta_map_final: Dict[str, Tuple[str, str]],
    template_row: Dict[str, str],
):
    """
    Builds the Action block code cell that applies FINAL DB changes.
    Now starts with an Imports section mirroring the Initial DB block,
    but only for final_services + their dependencies.
    """
    L: List[str] = []

    # --- Imports for Action block (mirrors Initial DB block) ---
    action_api_modules = api_modules_for_services(final_services)
    L.append("# Imports (Action)")
    for m in action_api_modules: L.append(f"import {m}")
    if "notes_and_lists" in action_api_modules:
        L.append("from notes_and_lists.SimulationEngine.utils import update_title_index, update_content_index")
        L.append("from typing import Dict, Any")
        L.append("from datetime import timezone")
    L += ["import json, uuid", "from datetime import datetime", "import os", ""]
    # (We do NOT reload default DBs or reset USER_LOCATION here.)

    calls: List[str] = []

    if not final_services:
        L += ["# No final state changes requested for this task.", ""]
        return new_code_cell("\n".join(L) + "\n")

    for svc_raw in final_services:
        svc = normalize_service_token(svc_raw)
        spec = PORTING_SPECS.get(svc)
        if not spec:
            L += [f"# (No porting spec defined for '{svc}'; skipping)", ""]
            continue

        var_name, is_dict = SELF_VAR_BY_SERVICE.get(svc, (None, False))
        if not var_name:
            L += [f"# (Could not determine primary variable for service '{svc}'; skipping)", ""]
            continue

        # --- Inject FINAL JSON for the service from Working Sheet ---
        col = final_db_col_for_service(svc)   # <service>_final_db from Working Sheet
        d = parse_json_best_effort(working_row.get(col))
        if is_dict:
            L += [f"# {var_name} from Working Sheet → {col} (dict)",
                  f"{var_name} = {py_literal(d)}", ""]
        else:
            L += [f"# {var_name} from Working Sheet → {col} (JSON string)",
                  f"{var_name} = json.dumps({py_literal(d)}, ensure_ascii=False)", ""]

        # --- WhatsApp-specific handling for contacts_src_json ---
        if svc == "whatsapp":
            contacts_final_col = final_db_col_for_service("contacts")  # 'contacts_final_db'
            contacts_final_raw = (working_row.get(contacts_final_col) or "").strip()
            if contacts_final_raw:
                contacts_final_parsed = parse_json_best_effort(contacts_final_raw)
                L += [
                    "# Use contacts from Working Sheet final DB for WhatsApp final stage",
                    f"contacts_src_json = json.dumps({py_literal(contacts_final_parsed)}, ensure_ascii=False)",
                    "",
                ]
            else:
                # Fall back to Template Colab contacts_initial_db
                try:
                    contacts_dict = parse_initial_db(template_row.get('contacts_initial_db'))
                except Exception:
                    contacts_dict = {}
                L += [
                    "# Ensure contacts_src_json for WhatsApp final stage (fallback from Template Colab contacts_initial_db)",
                    f"contacts_src_json = json.dumps({py_literal(contacts_dict)}, ensure_ascii=False)",
                    "",
                ]

        # --- Append FINAL-DB porting function code (if any) ---
        code_str = code_map_final.get(svc, "")
        if code_str:
            code_str = reescape_newlines_inside_string_literals(code_str).strip()
            date_upd, resp = meta_map_final.get(svc, ("", ""))
            L += [
                f"# ==== Final-DB porting code for service: {svc} (from live sheet: function_to_translate_json_finalDB) ====",
                f"# Using latest FINAL-DB code for '{svc}' which was updated on {date_upd} by {resp}",
                code_str,
                "",
            ]
        else:
            L += [f"# (No final-DB code in code sheet for service '{svc}')", ""]

        # Use SAME call as initial stage
        for ln in spec.get("pre_call_lines", []): L.append(ln)
        if spec.get("pre_call_lines"): L.append("")
        calls.append(spec["call"])

    if calls:
        L += ["# Execute final porting"] + calls
    return new_code_cell("\n".join(L) + "\n")

def build_initial_assertion_comment_cell(
    services_for_code: List[str],
    final_services: List[str],
    template_row: Dict[str, str],
    working_row: Dict[str, str],
    code_map_final: Dict[str, str],
) -> nbformat.NotebookNode:
    lines: List[str] = []
    lines.append("# === Notebook summary (commented; no execution) ===")
    lines.append("# INITIAL DB → services (detected from *_initial_db columns) and input columns used:")
    if services_for_code:
        for svc in services_for_code:
            spec = PORTING_SPECS.get(svc, {})
            cols = [c for (c, _var, _d) in spec.get("json_vars", [])]
            present = [c for c in cols if str(template_row.get(c, "")).strip()]
            lines.append(f"#   - {svc}: columns={cols or '[]'} present={present or '[]'}")
    else:
        lines.append("#   - (none)")

    lines.append("#")
    lines.append("# FINAL DB → requested services and availability:")
    if final_services:
        for raw in final_services:
            svc = normalize_service_token(raw)
            col = final_db_col_for_service(svc)
            has_json = bool(str(working_row.get(col, "")).strip())
            has_code = bool(code_map_final.get(svc, "").strip())
            extra = ""
            if svc == "whatsapp":
                contacts_final_col = final_db_col_for_service("contacts")
                has_contacts_final = bool(str(working_row.get(contacts_final_col, "")).strip())
                if has_contacts_final:
                    extra = "  (WhatsApp will use Working Sheet 'contacts_final_db')"
                else:
                    extra = "  (WhatsApp falls back to Template Colab 'contacts_initial_db')"
            lines.append(f"#   - {svc}: final_db_col='{col}', json_present={has_json}, final_code_present={has_code}{extra}")
    else:
        lines.append("#   - (none)")

    lines.append("#")
    lines.append("# SERVICE CHANGES SUMMARY (requested vs expected):")
    lines.append("#   - requested: value(s) in Working Sheet column 'final_state_changes_needed'")
    lines.append("#   - applied: executed during this notebook's 'Action' block using the same porting calls.")
    lines.append("#   - NOTE: This is an informational comment; verify actual results below in Final Assertion.")

    return new_code_cell("\n".join(lines) + "\n")

def build_final_assertion_cell(working_row: Dict[str, str]) -> nbformat.NotebookNode:
    mod_code = (working_row.get(MODIFIED_FINAL_ASSERTION_COL_NAME) or "").strip()
    base_code = (working_row.get(FINAL_ASSERTION_COL_NAME) or "").strip()
    chosen = mod_code if mod_code else base_code
    chosen = reescape_newlines_inside_string_literals(chosen).strip()
    return new_code_cell(chosen + ("\n" if chosen else ""))

def build_golden_answer_cell(working_row: Dict[str, str]) -> nbformat.NotebookNode:
    """
    Markdown cell showing the Golden Answer header and the text from 'final_golden_response'.
    """
    golden = (working_row.get("final_golden_response") or "").strip()
    if golden:
        content = "# Golden Answer\n\n " + golden
    else:
        content = "# Golden Answer\n\n### (empty)"
    return new_markdown_cell(content)

def build_empty_block(title: str):
    return [new_markdown_cell(f"# {title}"), new_code_cell("")]

# ---------- Preflight & notebook generator
def preflight_row_ws(template_row: Dict[str, str], selected_services: List[str]) -> Dict[str, Any]:
    issues = {"unknown_services": [], "missing_inputs": [], "json_errors": {}}
    expanded = list(selected_services)
    for s in selected_services:
        spec = SERVICE_SPECS.get(s)
        if spec:
            for req in spec.get("requires", []):
                if req not in expanded:
                    expanded.append(req)
    issues["unknown_services"] = [s for s in expanded if s not in SERVICE_SPECS]

    need = sorted({c for s in expanded for c in PORTING_SPECS.get(s, {}).get("json_vars", [])})
    colnames = [c[0] for c in need]
    for col in colnames:
        v = template_row.get(col, "")
        if not str(v).strip():
            issues["missing_inputs"].append(col)
        else:
            try:
                json.loads(str(v))
            except Exception as e:
                issues["json_errors"][col] = str(e)
    return {"expanded": expanded, "issues": issues}

def _parse_public_tools(public_tools_str: str) -> list[str]:
    """
    Parse the 'public_content_sources_used' string into a normalized list of tool identifiers.
    
    Examples:
        "Google Search" -> ["google_search"]
        "Google Maps" -> ["google_maps_live"]
        "YouTube" -> ["youtube_tool"]
        "Google Search | Google Maps" -> ["google_search", "google_maps_live"]
    """
    if not public_tools_str:
        return []

    # Split on "|" and normalize spacing
    raw_tools = [part.strip() for part in public_tools_str.split("|") if part.strip()]

    # Mapping from  names -> internal identifiers
    mapping = {
        "Google Search": "google_search",
        "Google Maps": "google_maps_live",
        "YouTube": "youtube_tool",
    }

    # Normalize using mapping (skip unknowns gracefully)
    return [mapping[tool] for tool in raw_tools if tool in mapping]


def generate_notebook_for_row_ws(
    working_row: Dict[str, str],
    template_row: Dict[str, str],
    idx: int,
    setup_cell: str,
    pipinstall_cell: str,
    code_map_initial: Dict[str, str],
    meta_map_initial: Dict[str, Tuple[str, str]],
    code_map_final: Dict[str, str],
    meta_map_final: Dict[str, Tuple[str, str]],
) -> Tuple[nbformat.NotebookNode, Dict[str, Any], str]:
    # DATA-DRIVEN initial services
    services_for_code = compute_initial_services_from_template_row(template_row)

    pre = preflight_row_ws(template_row, services_for_code)
    expanded = pre["expanded"]; issues = pre["issues"]

    # API modules list (selected + dependencies)
    api_modules: List[str] = api_modules_for_services(expanded)

    sample_id = (working_row.get("Sample ID") or working_row.get("sample_id") or working_row.get("SampleID") or "").strip() or f"row-{idx}"
    query_txt = (working_row.get("query") or "").strip()
    user_loc = working_row.get("user_location", "")
    query_date = (working_row.get("query_date") or "").strip()
    uploaded_file_url = (working_row.get("video_prompt") or "").strip()
    public_tools = _parse_public_tools((working_row.get("public_content_sources_used") or "").strip())
    final_services = split_services(working_row.get("final_state_changes_needed", ""))


    nb = new_notebook()
    nb.cells.append(build_metadata_cell(sample_id, query_txt, api_modules, query_date,uploaded_file_url,public_tools))
    w = build_warnings_cell(issues)
    if w: nb.cells.append(w)
    nb.cells.append(new_markdown_cell("# Set Up"))
    nb.cells.extend(build_setup_cells(setup_cell, pipinstall_cell))

    nb.cells.append(new_markdown_cell("## Import APIs and initiate DBs"))
    nb.cells.append(
        build_import_and_port_cell_ws(
            api_modules=api_modules,
            services_for_code=services_for_code,
            template_row=template_row,
            code_map_initial=code_map_initial,
            meta_map_initial=meta_map_initial,
            user_location_value=user_loc,
            query_date=query_date.
            public_tools=public_tools
        )
    )

    # Initial Assertion — commented summary
    nb.cells.append(new_markdown_cell("# Initial Assertion"))
    nb.cells.append(
        build_initial_assertion_comment_cell(
            services_for_code=services_for_code,
            final_services=final_services,
            template_row=template_row,
            working_row=working_row,
            code_map_final=code_map_final,
        )
    )

    # Action block: FINAL DB porting (with imports)
    nb.cells.append(new_markdown_cell("# Action"))
    nb.cells.append(build_action_final_dbs_cell_ws(final_services, working_row, code_map_final, meta_map_final, template_row))

    # Golden Answer (markdown) — right after Action, before Final Assertion
    nb.cells.append(build_golden_answer_cell(working_row))

    nb.cells.append(new_markdown_cell("# Final Assertion"))
    nb.cells.append(new_code_cell(''))

    nb.metadata["colab"] = {"provenance": []}
    nb.metadata["language_info"] = {"name": "python"}
    return nb, issues, sample_id

def add_freezegun_block(L,query_date):
    """
    Adds the freezegun code snippet block
    """
    # add freeze gun if we have query_date 
    if not query_date:
        return L

    # verify the query_date is in valid format 
    try:
        parser.parse(query_date)
    except Exception as e:
        print(f"Failed to parse query date : {query_date} | skipping freezegun ..")
        return L
    
    # Add Freezegun as first import
    L.append("### Freezegun Block Start")
    L.append("import freezegun")
    L.append("from freezegun import freeze_time")
    L.append("")  # Empty line for readability
    # Add the start_frozen_time function definition
    L.extend([
        "def start_frozen_time(current_date):",
        '    """',
        '    Starts a frozen time context using freezegun.',
        '    """',
        '    ignore_pkgs = {\"ipykernel\", \"ipyparallel\", \"ipython\", \"jupyter-server\"}',
        '    freezegun.configure(extend_ignore_list=list(ignore_pkgs))',
        '    freezer = freeze_time(current_date)',
        '    freezer.start()',
        '    return freezer',
        ""  # Empty line after function
    ])
    L.append(f'current_time = "{query_date}"')
    L.append("start_frozen_time(current_time)")
    L.append("from datetime import datetime")
    L.append('print("--> FROZEN TIME:", datetime.now())')
    L.append("### Freezegun Block End")
    L.append("")  # Empty line for readability

    return L

def add_gemini_keys(L, public_tools):
    if public_tools:
        if GEMINI_API_KEY and DEFAULT_GEMINI_MODEL_NAME and LIVE_API_URL:
            # Add Gemini keys if only public tools are used
            L.append("### Public Live Tools Env")
            L.append("import os")
            L.append(f"os.environ['GEMINI_API_KEY'] = '{GEMINI_API_KEY}'")
            L.append(f"os.environ['GOOGLE_API_KEY'] = '{GEMINI_API_KEY}'")
            L.append(f"os.environ['DEFAULT_GEMINI_MODEL_NAME'] = '{DEFAULT_GEMINI_MODEL_NAME}'")
            L.append(f"os.environ['LIVE_API_URL'] = '{LIVE_API_URL}'")
        else:
            raise ValueError(
                "Failed to generate templates. "
                "Required `GEMINI_API_KEY` & `DEFAULT_GEMINI_MODEL_NAME` to use public tools."
            )
    return L

# ---------- Parallel worker
def build_and_upload_worker(
    idx: int,
    working_row: Dict[str, str],
    template_row: Dict[str, str],
    setup_cell: str,
    pipinstall_cell: str,
    code_map_initial: Dict[str, str],
    meta_map_initial: Dict[str, Tuple[str, str]],
    code_map_final: Dict[str, str],
    meta_map_final: Dict[str, Tuple[str, str]],
    out_folder_id: str,
):
    task_id = (working_row.get("task_id") or f"row-{idx}").strip() or f"row-{idx}"
    try:
        nb, issues, sample_id = generate_notebook_for_row_ws(
            working_row, template_row, idx, setup_cell, pipinstall_cell,
            code_map_initial, meta_map_initial, code_map_final, meta_map_final
        )
        safe_name = re.sub(r"[\\/:*?\"<>|]+", "_", sample_id).strip() or f"row-{idx}"
        fname = f"{safe_name}.ipynb"
        _, colab_url = upload_notebook_to_drive_with_retries(out_folder_id, fname, nb)
        services_required_for_summary = (working_row.get("services_needed") or "").strip()
        return (idx, sample_id, task_id, services_required_for_summary, colab_url, issues, None)
    except Exception as e:
        tb = traceback.format_exc()
        log.error("Worker failed for task_id=%s: %s\n%s", task_id, e, tb)
        sample_id = (working_row.get("Sample ID") or f"row-{idx}").strip()
        return (idx, sample_id, task_id, (working_row.get("services_needed") or "").strip(), "", None, e)

# ---------- Main
def main():
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID is required. Set os.environ['SPREADSHEET_ID'] before running.")
    if not CODE_SHEET_NAME:
        raise RuntimeError("CODE_SHEET_NAME is required. Set os.environ['CODE_SHEET_NAME'].")

    setup_cell, pipinstall_cell = mount_and_import_codebase()
    log.info("Mounted Drive and imported static setup cells from %s", CODEBASE_ROOT)

    drive, sheets = auth_services()
    log.info("Authenticated to Google Drive & Sheets APIs.")

    out_folder_id = resolve_output_folder_id(drive, WS_OUT_FOLDER_NAME, CODEBASE_FOLDER_NAME)
    log.info("Output notebooks Drive folder id: %s", out_folder_id)
    empty_drive_folder(drive, out_folder_id)

    ws_name = SOURCE_WORKING_SHEET_NAME or get_first_sheet_title(sheets, SPREADSHEET_ID)
    log.info("Reading Working Sheet rows from '%s' (spreadsheet id: %s)", ws_name, SPREADSHEET_ID)
    _, ws_rows = read_sheet_as_dicts(sheets, SPREADSHEET_ID, ws_name)
    if not ws_rows:
        log.warning("No data found in Working Sheet.")
        return
    log.info("Loaded %d working rows.", len(ws_rows))

    templ_name = SOURCE_SHEET_NAME or get_first_sheet_title(sheets, SPREADSHEET_ID)
    log.info("Reading Template Colab rows from '%s'", templ_name)
    _, templ_rows = read_sheet_as_dicts(sheets, SPREADSHEET_ID, templ_name)
    templ_by_task: Dict[str, Dict[str, str]] = { (r.get("task_id") or "").strip(): r for r in templ_rows if (r.get("task_id") or "").strip() }

    code_sheet_id = CODE_SPREADSHEET_ID or SPREADSHEET_ID
    log.info("Reading INITIAL porting code from '%s' (spreadsheet id: %s)", CODE_SHEET_NAME, code_sheet_id)
    code_map_initial, meta_map_initial = build_service_code_map_with_logs(
        sheets,
        spreadsheet_id=code_sheet_id,
        code_sheet_name=CODE_SHEET_NAME,
        code_col_candidates=["function_to_translate_json","code","porting_code","port_code"],
    )
    log.info("Prepared INITIAL porting code for %d services.", len(code_map_initial))

    log.info("Reading FINAL-DB porting code from '%s' (spreadsheet id: %s)", CODE_SHEET_NAME, code_sheet_id)
    code_map_final, meta_map_final = build_service_code_map_with_logs(
        sheets,
        spreadsheet_id=code_sheet_id,
        code_sheet_name=CODE_SHEET_NAME,
        code_col_candidates=["function_to_translate_json_finalDB","final_db_code","final_porting_code"],
    )
    log.info("Prepared FINAL-DB porting code for %d services.", len(code_map_final))

    start = time.time()
    rows_for_summary: List[Tuple[int, str, str, str, str]] = []
    problems_cnt = 0

    work_items = []
    for i, wrow in enumerate(ws_rows, start=1):
        task_id = (wrow.get("task_id") or f"row-{i}").strip() or f"row-{i}"
        trow = templ_by_task.get(task_id, {})
        work_items.append((i, wrow, trow))

    log.info("Starting parallel build/upload with %d worker(s)…", MAX_WORKERS)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="ws") as ex:
        futures = [
            ex.submit(
                build_and_upload_worker,
                i, wrow, trow,
                setup_cell, pipinstall_cell,
                code_map_initial, meta_map_initial,
                code_map_final,  meta_map_final,
                out_folder_id,
            )
            for (i, wrow, trow) in work_items
        ]

        for fut in as_completed(futures):
            idx, sample_id, task_id, services_required, colab_url, issues, err = fut.result()
            if err is None:
                rows_for_summary.append((idx, sample_id, task_id, services_required, colab_url))
                if issues and (issues.get("unknown_services") or issues.get("missing_inputs") or issues.get("json_errors")):
                    problems_cnt += 1
                    if issues["unknown_services"]:
                        log.warning("Unknown services for %s: %s", task_id, ", ".join(issues["unknown_services"]))
                    if issues["missing_inputs"]:
                        log.warning("Missing inputs for %s: %s", task_id, ", ".join(issues["missing_inputs"]))
                    if issues["json_errors"]:
                        for col, err_txt in issues["json_errors"].items():
                            log.warning("JSON error in %s for %s: %s", col, task_id, err_txt)
                log.info("✔ Uploaded %s (%s) → %s", sample_id, task_id, colab_url)
            else:
                problems_cnt += 1
                rows_for_summary.append((idx, sample_id, task_id, services_required, ""))  # keep row; empty URL on failure
                log.error("✖ Failed %s / %s (kept in summary with empty URL).", sample_id, task_id)

    rows_for_summary.sort(key=lambda x: x[0])
    rows_final = [[sample_id, task_id, services, url] for _, sample_id, task_id, services, url in rows_for_summary]

    sheets = build("sheets", "v4")
    upsert_summary_sheet_ws(sheets, SPREADSHEET_ID, SUMMARY_SHEET_NAME_WORKING_AUTOMATION, rows_final)

    elapsed = time.time() - start
    log.info("Parallel generation complete in %.1fs with %d problem row(s).", elapsed, problems_cnt)

# if __name__ == "__main__":
#     main()
