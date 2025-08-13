# generator_drive.py
# Live Google Sheet → Colab notebooks in Drive + summary tab (Notes & Reminders fixed)

from __future__ import annotations

import os, sys, io, re, json, csv, pprint, importlib
from typing import Dict, Any, List, Optional, Tuple

import nbformat
from nbformat.v4 import new_notebook, new_markdown_cell, new_code_cell

# --- Colab / Google APIs ---
from google.colab import auth, drive as gdrive_mount
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload

# =========================
# Config via environment
# =========================
SPREADSHEET_ID     = os.environ.get("SPREADSHEET_ID", "").strip()       # REQUIRED
SOURCE_SHEET_NAME  = os.environ.get("SOURCE_SHEET_NAME", "").strip()     # optional
SUMMARY_SHEET_NAME = os.environ.get("SUMMARY_SHEET_NAME", "Generated_Colabs").strip()
OUT_FOLDER_NAME    = os.environ.get("OUT_FOLDER_NAME", "generated_colabs").strip()

# Drive paths
MYDRIVE_ROOT         = "/content/drive/MyDrive"
CODEBASE_FOLDER_NAME = "port_automation"               # your codebase folder in Drive
CODEBASE_ROOT        = os.path.join(MYDRIVE_ROOT, CODEBASE_FOLDER_NAME)

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
# Porting specs (module → code var → injected vars → call)
# json_vars: (sheet_column, notebook_var_name, inject_as_dict)
# =========================
PORTING_SPECS: Dict[str, Dict[str, Any]] = {
    "whatsapp": {
        "import_path": "apis_porting_code.whatsapp_contacts_port",
        "code_var":    "whatsapp_contacts_port",
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
        "import_path": "apis_porting_code.calendar_port",
        "code_var":    "calendar_port",
        "json_vars":   [("calendar_initial_db", "port_calender_db", True)],  # inject as dict
        "call":        "port_calendar_db(json.dumps(port_calender_db, ensure_ascii=False))",
    },
    "contacts": {
        "import_path": "apis_porting_code.contact_port",
        "code_var":    "contact_port",
        "json_vars":   [("contacts_initial_db", "contacts_src_json", False)],
        "call":        "port_db_contacts(contacts_src_json)",
    },
    "gmail": {
        "import_path": "apis_porting_code.gmail_port",
        "code_var":    "gmail_port",
        "json_vars":   [("gmail_initial_db", "gmail_src_json", False)],
        "pre_call_lines": ["port_gmail_db_key = gmail_src_json"],
        "call":        "port_gmail_db(port_gmail_db_key)",
    },
    "device_settings": {
        "import_path": "apis_porting_code.device_settings_port",
        "code_var":    "device_settings_port",
        "json_vars":   [("device_settings_initial_db", "device_settings_src_json", False)],
        "call":        "port_device_setting_db(device_settings_src_json)",
    },
    "media_control": {
        "import_path": "apis_porting_code.media_control_port",
        "code_var":    "media_control_port",
        "json_vars":   [("media_control_initial_db", "media_control_src_json", False)],
        "call":        "port_media_control_db(media_control_src_json)",
    },
    "clock": {
        "import_path": "apis_porting_code.clock_port",
        "code_var":    "clock_port",
        "json_vars":   [("clock_initial_db", "clock_src", False)],  # << pass dict
        "call":        "port_clock_db(clock_src)",
    },
    "reminders": {
        "import_path": "apis_porting_code.generic_reminders_port",
        "code_var":    "generic_reminders_port",
        "json_vars":   [("reminders_initial_db", "reminders_src_json", False)],
        "call":        "port_generic_reminder_db(reminders_src_json)",
    },
    "notes": {
        "import_path": "apis_porting_code.notes_and_lists_port",
        "code_var":    "notes_and_lists_port",
        "json_vars":   [("notes_initial_db", "notes_src_json", False)],
        "call":        "port_notes_and_lists_initial_db(notes_src_json)",
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
}

# =========================
# Helpers
# =========================
def mount_and_import_codebase():
    # Mount Google Drive
    gdrive_mount.mount("/content/drive", force_remount=False)
    if not os.path.isdir(CODEBASE_ROOT):
        raise RuntimeError(f"Codebase folder not found at {CODEBASE_ROOT}. "
                           f"Place your repo under MyDrive/{CODEBASE_FOLDER_NAME}/")
    # Add to sys.path
    if CODEBASE_ROOT not in sys.path:
        sys.path.append(CODEBASE_ROOT)

    # Import static cells
    from static_code.setup_code_cell import setup_cell
    from static_code.pipinstall_cell import pipinstall_cell
    return setup_cell, pipinstall_cell

def auth_services():
    auth.authenticate_user()
    drive = build("drive", "v3")
    sheets = build("sheets", "v4")
    return drive, sheets

def normalize_service_token(tok: str) -> str:
    t = re.sub(r'[/&]', ' ', str(tok).strip().lower())
    t = re.sub(r'\s+', ' ', t)
    synonyms = {
        'google calendar': 'calendar', 'calender': 'calendar',
        'google mail': 'gmail', 'email': 'gmail', 'e-mail': 'gmail',
        'media control': 'media_control',
        'device settings': 'device_settings',
        'whatsapp message': 'whatsapp', 'whatsapp messages': 'whatsapp',
        'message': 'whatsapp', 'messages': 'whatsapp',
        'reminder': 'reminders', 'generic reminders': 'reminders',
        'notes and lists': 'notes', 'notes_and_lists': 'notes',
    }
    return synonyms.get(t, t)

def split_services(cell: Optional[str]) -> List[str]:
    if not cell: return []
    tokens = re.split(r'[|,]', cell)
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
    return json.loads(s)  # strict JSON → True/False/None handled

def py_literal(obj: Any) -> str:
    return pprint.pformat(obj, width=100, sort_dicts=False)

def reescape_newlines_inside_string_literals(src: str) -> str:
    """Re-escape real newlines in quoted strings → '\\n' (keep prints intact)."""
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
                    out+= [ch,quote,quote]; i+=3; in_str=triple=False
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
                out+= [ch,ch,ch]; i+=3; in_str=True; triple=True; quote=ch
            else:
                out.append(ch); i+=1; in_str=True; triple=False; quote=ch
            continue
        out.append(ch); i+=1
    return "".join(out)

def try_import_code_string(import_path: str, var_name: str) -> Optional[str]:
    try:
        mod = importlib.import_module(import_path)
        code_str = getattr(mod, var_name)
        return code_str if isinstance(code_str, str) else None
    except Exception:
        return None

# ---------- Google Sheets + Drive helpers

def get_first_sheet_title(sheets, spreadsheet_id: str) -> str:
    meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    return meta["sheets"][0]["properties"]["title"]

def read_sheet_as_dicts(sheets, spreadsheet_id: str, sheet_name: str) -> Tuple[List[str], List[Dict[str,str]]]:
    rng = f"'{sheet_name}'"
    resp = sheets.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    values = resp.get("values", [])
    if not values:
        return [], []
    headers = [h.strip() for h in values[0]]
    rows=[]
    for r in values[1:]:
        d = {headers[i]: (r[i] if i < len(r) else "") for i in range(len(headers))}
        rows.append(d)
    return headers, rows

def ensure_subfolder(drive, parent_id: str, name: str) -> str:
    # find
    q = f"'{parent_id}' in parents and name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    res = drive.files().list(q=q, fields="files(id,name)").execute().get("files", [])
    if res:
        return res[0]["id"]
    # create
    meta = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
    folder = drive.files().create(body=meta, fields="id").execute()
    return folder["id"]

def find_root_folder_id(drive, name: str) -> str:
    # Look in My Drive root
    q = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and 'root' in parents and trashed=false"
    res = drive.files().list(q=q, fields="files(id,name)").execute().get("files", [])
    if not res:
        raise RuntimeError(f"Folder '{name}' not found in My Drive root.")
    return res[0]["id"]

def upload_notebook_to_drive(drive, folder_id: str, filename: str, nb: nbformat.NotebookNode) -> Tuple[str, str]:
    data = nbformat.writes(nb).encode("utf-8")
    media = MediaInMemoryUpload(data, mimetype="application/vnd.google.colaboratory", resumable=False)
    meta = {"name": filename, "mimeType": "application/vnd.google.colaboratory", "parents": [folder_id]}
    file = drive.files().create(body=meta, media_body=media, fields="id,webViewLink").execute()
    file_id = file["id"]
    colab_url = f"https://colab.research.google.com/drive/{file_id}"
    return file_id, colab_url

def upsert_summary_sheet(sheets, spreadsheet_id: str, sheet_name: str, headers: List[str], rows_plus_url: List[List[str]]):
    # ensure sheet exists (or clear)
    meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    existing = {sh["properties"]["title"] for sh in meta.get("sheets", [])}
    if sheet_name not in existing:
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
        ).execute()
    else:
        sheets.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=f"'{sheet_name}'"
        ).execute()

    final_headers = headers + ["colab_url"]
    body = {"range": f"'{sheet_name}'!A1", "majorDimension": "ROWS", "values": [final_headers] + rows_plus_url}
    sheets.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!A1",
        valueInputOption="RAW",
        body=body
    ).execute()

# ---------- Preflight & notebook builders

def preflight_row(row: Dict[str,str]) -> Dict[str, Any]:
    issues = {"unknown_services":[], "missing_inputs":[], "json_errors":{}}
    services = split_services(row.get("services_needed",""))
    expanded = list(services)
    for s in services:
        spec = SERVICE_SPECS.get(s)
        if spec:
            for req in spec["requires"]:
                if req not in expanded: expanded.append(req)
    issues["unknown_services"] = [s for s in expanded if s not in SERVICE_SPECS]
    need = sorted({c for s in expanded for c in REQUIRED_INPUTS.get(s,[])})
    for col in need:
        v = row.get(col,"")
        if not str(v).strip():
            issues["missing_inputs"].append(col)
        else:
            try: json.loads(str(v))
            except Exception as e: issues["json_errors"][col]=str(e)
    return {"services":services, "expanded":expanded, "issues":issues}

def build_metadata_cell(task_id: str, api_modules: List[str]):
    md = ["# Sample ID\n\n",
          f"**Sample ID**: {task_id}\n\n",
          "**Query**:\n\n",
          "**DB Type**: Base Case\n\n",
          "**Case Description**:\n\n",
          "**Global/Context Variables:**\n\n",
          "**APIs:**\n"]
    md += [f"- {a}\n" for a in api_modules]
    md.append("\n**Databases:**")
    return new_markdown_cell("".join(md))

def build_warnings_cell(issues: Dict[str, Any]):
    msgs=[]
    if issues["unknown_services"]:
        msgs.append(f"- Unknown/unsupported services: `{', '.join(issues['unknown_services'])}`")
    if issues["missing_inputs"]:
        msgs.append(f"- Missing required inputs: `{', '.join(issues['missing_inputs'])}`")
    if issues["json_errors"]:
        msgs.append("- JSON parse errors:\n  - " + "\n  - ".join(f"`{k}` → {v}" for k,v in issues["json_errors"].items()))
    return new_markdown_cell("### Warnings detected for this row\n\n" + "\n".join(msgs)) if msgs else None

def build_setup_cells(setup_cell: str, pipinstall_cell: str):
    setup_src = reescape_newlines_inside_string_literals(setup_cell).strip()+"\n"
    pip_src   = reescape_newlines_inside_string_literals(pipinstall_cell).strip()+"\n"
    return [
        new_markdown_cell("## Download relevant files"),
        new_code_cell(setup_src),
        new_markdown_cell("## Install Dependencies and Clone Repositories"),
        new_code_cell(pip_src),
    ]

def build_import_and_port_cell(api_modules: List[str], expanded_services: List[str], row: Dict[str,str]):
    L=[]
    L.append("# Imports")
    for m in api_modules: L.append(f"import {m}")
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

    calls=[]
    for svc in expanded_services:
        ps = PORTING_SPECS.get(svc)
        if not ps:
            L += [f"# (No porting spec defined for service '{svc}'; skipping)",""]
            continue

        for col, var, as_dict in ps.get("json_vars",[]):
            d = parse_initial_db(row.get(col))
            if as_dict:
                L += [f"# {var} from {col} (dict)", f"{var} = {py_literal(d)}", ""]
            else:
                L += [f"# {var} from {col} (JSON string)", f"{var} = json.dumps({py_literal(d)}, ensure_ascii=False)", ""]

        for ln in ps.get("pre_call_lines", []): L.append(ln)
        if ps.get("pre_call_lines"): L.append("")

        code_str = (
            try_import_code_string("apis_porting_code.whatsapp_contacts_port", "whatsapp_contacts_port") if svc=="whatsapp"
            else try_import_code_string("apis_porting_code.calendar_port", "calendar_port") if svc=="calendar"
            else try_import_code_string(ps["import_path"], ps["code_var"])
        )
        if code_str:
            code_str = reescape_newlines_inside_string_literals(code_str).strip()
            L += [f"# ==== Porting code for service: {svc} ====", code_str, ""]
            calls.append(ps["call"])
        else:
            L += [f"# (Port module missing or invalid for '{svc}': {ps['import_path']}.{ps['code_var']})",""]

    if calls:
        L += ["# Execute porting"] + calls
    return new_code_cell("\n".join(L)+"\n")

def build_empty_block(title: str): return [new_markdown_cell(f"# {title}"), new_code_cell("")]

def generate_notebook_for_row(row: Dict[str,str], idx: int, setup_cell: str, pipinstall_cell: str) -> Tuple[nbformat.NotebookNode, Dict[str, Any], List[str]]:
    pre = preflight_row(row)
    services = pre["services"]
    expanded = pre["expanded"]
    issues   = pre["issues"]

    # API modules list (respect implicit deps order)
    api_modules=[]
    for s in services:
        spec = SERVICE_SPECS.get(s)
        if not spec: continue
        api = spec["api"]
        if api not in api_modules: api_modules.append(api)
        for req in spec.get("requires", []):
            ra = SERVICE_SPECS[req]["api"]
            if ra not in api_modules: api_modules.append(ra)

    used_initial_cols = sorted({c for s in expanded for c in REQUIRED_INPUTS.get(s, [])})

    nb = new_notebook()
    task_id = (row.get("task_id") or f"row-{idx}").strip() or f"row-{idx}"
    nb.cells.append(build_metadata_cell(task_id, api_modules))
    w = build_warnings_cell(issues)
    if w: nb.cells.append(w)
    nb.cells.append(new_markdown_cell("# Set Up"))
    nb.cells.extend(build_setup_cells(setup_cell, pipinstall_cell))
    nb.cells.append(new_markdown_cell("## Import APIs and initiate DBs"))
    nb.cells.append(build_import_and_port_cell(api_modules, expanded, row))
    nb.cells.extend(build_empty_block("Initial Assertion"))
    nb.cells.extend(build_empty_block("Action"))
    nb.cells.extend(build_empty_block("Final Assertion"))
    nb.metadata["colab"]={"provenance": []}
    nb.metadata["language_info"]={"name":"python"}
    return nb, issues, used_initial_cols

# =========================
# Main
# =========================
def main():
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID is required. Set os.environ['SPREADSHEET_ID'] before running.")

    # Mount & import your codebase (static cells, port modules)
    setup_cell, pipinstall_cell = mount_and_import_codebase()

    # Auth to Google APIs
    drive, sheets = auth_services()

    # Find Drive folders: /MyDrive/port_automation and its generated subfolder
    codebase_folder_id = find_root_folder_id(drive, CODEBASE_FOLDER_NAME)
    out_folder_id      = ensure_subfolder(drive, codebase_folder_id, OUT_FOLDER_NAME)

    # Resolve source sheet
    src_name = SOURCE_SHEET_NAME or get_first_sheet_title(sheets, SPREADSHEET_ID)

    # Read sheet rows
    headers, rows = read_sheet_as_dicts(sheets, SPREADSHEET_ID, src_name)
    if not headers:
        print("No data found in source sheet.")
        return

    # Make output rows with same columns + colab_url
    header_index = {h:i for i,h in enumerate(headers)}
    output_rows_with_url: List[List[str]] = []

    problems = []

    for i, row in enumerate(rows, start=1):
        # Keep original order row values
        ordered_vals = [row.get(h,"") for h in headers]

        task_id = (row.get("task_id") or f"row-{i}").strip() or f"row-{i}"

        nb, issues, used_cols = generate_notebook_for_row(row, i, setup_cell, pipinstall_cell)
        fname = f"Gemini_Apps_ID_Data_Port_{task_id}.ipynb"
        file_id, colab_url = upload_notebook_to_drive(drive, out_folder_id, fname, nb)

        output_rows_with_url.append( ordered_vals + [colab_url] )

        if issues["unknown_services"] or issues["missing_inputs"] or issues["json_errors"]:
            problems.append((task_id, issues))

    # Write summary tab back to spreadsheet
    upsert_summary_sheet(sheets, SPREADSHEET_ID, SUMMARY_SHEET_NAME, headers, output_rows_with_url)

    print(f"Uploaded {len(output_rows_with_url)} notebooks → Drive folder '{OUT_FOLDER_NAME}' under '{CODEBASE_FOLDER_NAME}'.")
    print(f"Wrote summary tab: {SUMMARY_SHEET_NAME}")

    if problems:
        print("\nSome rows had issues:")
        for task_id, iss in problems:
            print(f"• {task_id}")
            if iss["unknown_services"]: print("  - Unknown services:", ", ".join(iss["unknown_services"]))
            if iss["missing_inputs"]:   print("  - Missing inputs:", ", ".join(iss["missing_inputs"]))
            if iss["json_errors"]:
                for col, err in iss["json_errors"].items():
                    print(f"  - JSON error in {col}: {err}")

if __name__ == "__main__":
    main()
