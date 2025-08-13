from __future__ import annotations

import csv, json, os, re, pprint, importlib
from typing import Dict, Any, List, Optional, Tuple
import nbformat
from nbformat.v4 import new_notebook, new_markdown_cell, new_code_cell

from static_code.setup_code_cell import setup_cell
from static_code.pipinstall_cell import pipinstall_cell

# Optional pre-imports for two known good blocks (fallback if importlib fails)
from apis_porting_code.whatsapp_contacts_port import whatsapp_contacts_port
from apis_porting_code.calendar_port import calendar_port

CSV_PATH = os.environ.get("CSV_PATH", "./sample_port_sheet.csv")
OUT_DIR  = os.environ.get("OUT_DIR", "./generated")

# Default DB paths (by package name)
DEFAULT_DB_PATHS: Dict[str, str] = {
    "contacts":           "DBs/ContactsDefaultDB.json",
    "whatsapp":           "/content/DBs/WhatsAppDefaultDB.json",
    "google_calendar":    "/content/DBs/CalendarDefaultDB.json",
    "gmail":              "/content/DBs/GmailDefaultDB.json",
    "device_setting":     "/content/DBs/DeviceSettingDefaultDB.json",   # singular
    "media_control":      "/content/DBs/MediaControlDefaultDB.json",
    "clock":              "/content/DBs/ClockDefaultDB.json",
    "generic_reminders":  "/content/DBs/GenericRemindersDefaultDB.json",
    "notes_and_lists":    "/content/DBs/NotesAndListsDefaultDB.json",
}

# Tracker service → runtime API package (+ implicit deps)
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

# Where to pull the code string from, what vars to inject, how to call
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
        "import_path": "apis_porting_code.contact_port",   # actual file/var in your zip
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
        "call":        "port_device_setting_db(device_settings_src_json)",  # singular
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
        "json_vars":   [("clock_initial_db", "clock_src_json", False)],
        "call":        "port_clock_db(clock_src_json)",
    },
    "reminders": {
        "import_path": "apis_porting_code.generic_reminders_port",   # actual file/var
        "code_var":    "generic_reminders_port",
        "json_vars":   [("reminders_initial_db", "reminders_src_json", False)],
        "call":        "port_generic_reminder_db(reminders_src_json)",  # actual func
    },
    "notes": {
        "import_path": "apis_porting_code.notes_and_lists_port",     # actual file/var
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

# ---------- helpers

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
    return json.loads(s)  # strict JSON (true/false/null handled)

def py_literal(obj: Any) -> str:
    return pprint.pformat(obj, width=100, sort_dicts=False)

def reescape_newlines_inside_string_literals(src: str) -> str:
    """Re-escape real newlines in quoted strings → '\\n', without touching prints in code."""
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

# ---------- preflight

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

# ---------- notebook builders

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

def build_setup_cells():
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
    # notes needs extra imports
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

        # paste the port code
        code_str = (
            whatsapp_contacts_port if svc=="whatsapp"
            else calendar_port if svc=="calendar"
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

def generate_notebook_for_row(row: Dict[str,str], idx: int) -> Tuple[nbformat.NotebookNode, Dict[str, Any], List[str]]:
    pre = preflight_row(row)
    services = pre["services"]
    expanded = pre["expanded"]
    issues   = pre["issues"]

    # resolve API modules (incl. implicit deps)
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
    nb.cells.extend(build_setup_cells())
    nb.cells.append(new_markdown_cell("## Import APIs and initiate DBs"))
    nb.cells.append(build_import_and_port_cell(api_modules, expanded, row))
    nb.cells.extend(build_empty_block("Initial Assertion"))
    nb.cells.extend(build_empty_block("Action"))
    nb.cells.extend(build_empty_block("Final Assertion"))
    nb.metadata["colab"]={"provenance": []}
    nb.metadata["language_info"]={"name":"python"}
    return nb, issues, used_initial_cols

def write_summary_csv(out_dir: str, rows: List[List[str]]) -> str:
    path = os.path.join(out_dir, "summary.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(rows)
    return path

def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    summary_rows = [["task_id", "services_required", "initial_db_fields_used", "colab_url"]]
    paths=[]; problems=[]

    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
            task_id = (row.get("task_id") or f"row-{i}").strip()
            if not task_id:
                continue
            nb, issues, used_cols = generate_notebook_for_row(row, i)
            fname = f"Gemini_Apps_ID_Data_Port_{task_id}.ipynb"
            out_path = os.path.join(OUT_DIR, fname)
            with open(out_path, "w", encoding="utf-8") as out:
                nbformat.write(nb, out)
            paths.append(out_path)

            services_required = " | ".join(split_services(row.get("services_needed","")))
            initial_db_fields_used = " | ".join(used_cols)
            summary_rows.append([task_id, services_required, initial_db_fields_used, os.path.abspath(out_path)])

            if issues["unknown_services"] or issues["missing_inputs"] or issues["json_errors"]:
                problems.append((task_id, issues))

    summary_path = write_summary_csv(OUT_DIR, summary_rows)

    print(f"Wrote {len(paths)} notebooks → {OUT_DIR}")
    for p in paths: print(" -", p)
    print(f"\nSummary CSV → {summary_path}")

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
