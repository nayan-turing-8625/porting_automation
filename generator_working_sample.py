# generator_working_sample.py
from __future__ import annotations

import os
from typing import Dict, Any, List, Tuple

import nbformat
from nbformat.v4 import new_notebook, new_markdown_cell, new_code_cell

from generator_utils import (
    log, CODEBASE_ROOT, CODEBASE_FOLDER_NAME,
    SERVICE_SPECS, REQUIRED_INPUTS,
    mount_and_import_codebase, auth_services,
    resolve_output_folder_id, empty_drive_folder, upload_notebook_to_drive,
    read_sheet_as_dicts, get_first_sheet_title,
    build_service_code_map_with_logs, services_from_initial_db_columns,
    build_setup_cells, build_import_and_port_cell, build_empty_block, build_warnings_cell,
    upsert_summary_sheet, _now_pacific
)

# ----------- ENV (separate knobs) -----------
SPREADSHEET_ID     = os.environ.get("SPREADSHEET_ID", "").strip()  # common
SRC_WS_NAME        = os.environ.get("SOURCE_WORKING_SHEET_NAME", "Working_Sheet_Test_Automation").strip()
CODE_SHEET_NAME    = os.environ.get("CODE_SHEET_NAME", "Translate_JSONs").strip()

SUMMARY_WS_NAME    = os.environ.get("SUMMARY_SHEET_NAME_WORKING_AUTOMATION", "Automated_Generated_Colabs").strip()
OUT_HINT           = os.environ.get("WS_OUT_FOLDER_NAME", "generated_colabs_working").strip()

# ----------- Notebook-specific blocks -----------
def build_metadata_cell(task_id: str, api_modules: List[str], query_text: str):
    md = [
        f"**Sample ID**: {task_id}\n\n",
        f"**Query**:\n\n{query_text}\n\n",
        "**DB Type**: Base Case\n\n",
        "**Case Description**:\n\n",
        "**Global/Context Variables:**\n\n",
        "**APIs:**\n",
    ]
    md += [f"- {a}\n" for a in api_modules]
    md.append("\n**Databases:**")
    return new_markdown_cell("".join(md))

def build_final_assertion_block(row: Dict[str, str]):
    code = (row.get("final_assertion_code") or "").strip()
    if code:
        from generator_utils import reescape_newlines_inside_string_literals
        code = reescape_newlines_inside_string_literals(code).strip() + "\n"
    return [new_markdown_cell("# Final Assertion"), new_code_cell(code)]

def generate_notebook_for_row(
    row: Dict[str, str],
    idx: int,
    setup_cell: str,
    pipinstall_cell: str,
    code_map: Dict[str, str],
    meta_map: Dict[str, Tuple[str, str]],
) -> Tuple[nbformat.NotebookNode, Dict[str, Any]]:
    selected = services_from_initial_db_columns(row)

    # expand + api_modules
    api_modules: List[str] = []
    expanded: List[str] = list(selected)
    for s in selected:
        spec = SERVICE_SPECS.get(s)
        if spec:
            for r in spec.get("requires", []):
                if r not in expanded:
                    expanded.append(r)
    for s in expanded:
        spec = SERVICE_SPECS.get(s)
        if spec:
            api = spec["api"]
            if api not in api_modules:
                api_modules.append(api)

    # validate
    issues = {"unknown_services": [s for s in expanded if s not in SERVICE_SPECS], "missing_inputs": [], "json_errors": {}}
    import json as _json
    needed = sorted({c for s in expanded for c in REQUIRED_INPUTS.get(s, [])})
    for col in needed:
        v = row.get(col, "")
        if not str(v).strip():
            issues["missing_inputs"].append(col)
        else:
            try: _json.loads(str(v))
            except Exception as e: issues["json_errors"][col] = str(e)

    task_id = (row.get("task_id") or f"row-{idx}").strip() or f"row-{idx}"
    query_text = (row.get("query") or "").strip()

    nb = new_notebook()
    nb.cells.append(build_metadata_cell(task_id, api_modules, query_text))
    w = build_warnings_cell(issues)
    if w: nb.cells.append(w)

    nb.cells.append(new_markdown_cell("# Set Up"))
    nb.cells.extend(build_setup_cells(setup_cell, pipinstall_cell))

    nb.cells.append(new_markdown_cell("## Import APIs and initiate DBs"))
    nb.cells.append(build_import_and_port_cell(api_modules, expanded, row, code_map, meta_map))

    nb.cells.extend(build_empty_block("Initial Assertion"))
    nb.cells.extend(build_empty_block("Action"))
    nb.cells.extend(build_final_assertion_block(row))

    nb.metadata["colab"] = {"provenance": []}
    nb.metadata["language_info"] = {"name": "python"}
    return nb, issues

# ----------- MAIN -----------
def main():
    if not SPREADSHEET_ID: raise RuntimeError("SPREADSHEET_ID is required.")
    if not CODE_SHEET_NAME: raise RuntimeError("CODE_SHEET_NAME is required.")

    setup_cell, pipinstall_cell = mount_and_import_codebase()
    log.info("Mounted Drive and imported static setup cells from %s", CODEBASE_ROOT)

    drive, sheets = auth_services()
    log.info("Authenticated to Google Drive & Sheets APIs.")

    out_folder_id = resolve_output_folder_id(drive, OUT_HINT, CODEBASE_FOLDER_NAME)
    log.info("Output notebooks Drive folder id: %s", out_folder_id)
    empty_drive_folder(drive, out_folder_id)

    src_name = SRC_WS_NAME or get_first_sheet_title(sheets, SPREADSHEET_ID)
    log.info("Reading tasks from working sheet '%s'", src_name)

    headers, rows = read_sheet_as_dicts(sheets, SPREADSHEET_ID, src_name)
    if not rows:
        log.warning("No rows in working sheet.")
        return
    log.info("Loaded %d rows.", len(rows))

    # live code, from same spreadsheet (shared sheet name)
    code_map, meta_map = build_service_code_map_with_logs(sheets, SPREADSHEET_ID, CODE_SHEET_NAME)

    # produce notebooks
    results: List[List[str]] = []  # task_id, query_order, colab_url
    for i, row in enumerate(rows, start=1):
        task_id = (row.get("task_id") or f"row-{i}").strip() or f"row-{i}"
        query_order = (row.get("query_order") or "").strip() or str(i)

        selected = services_from_initial_db_columns(row)
        log.info("---- Generating (working) for task_id=%s (turn=%s) ----", task_id, query_order)
        log.info("Selected services: %s", " | ".join(selected) if selected else "(none)")
        log.info("Query: %s", (row.get("query") or "").strip())

        nb, issues = generate_notebook_for_row(row, i, setup_cell, pipinstall_cell, code_map, meta_map)
        fname = f"Gemini_Apps_Data_Port_{task_id}_turn{query_order}.ipynb"
        _, colab_url = upload_notebook_to_drive(drive, out_folder_id, fname, nb)
        log.info("Uploaded: %s", fname)
        log.info("Colab URL: %s", colab_url)
        results.append([task_id, query_order, colab_url])

        if any(issues.values()):
            if issues["unknown_services"]: log.warning("Unknown services for %s: %s", task_id, ", ".join(issues["unknown_services"]))
            if issues["missing_inputs"]:   log.warning("Missing inputs for %s: %s", task_id, ", ".join(issues["missing_inputs"]))
            if issues["json_errors"]:
                for col, err in issues["json_errors"].items():
                    log.warning("JSON error in %s for %s: %s", col, task_id, err)

    # summary: task_id, query_order, colab_url, refresh_date, refresh_time
    refresh_date, refresh_time = _now_pacific()
    headers = ["task_id", "query_order", "colab_url", "refresh_date", "refresh_time"]
    rows = [r + [refresh_date, refresh_time] for r in results]
    upsert_summary_sheet(sheets, SPREADSHEET_ID, SUMMARY_WS_NAME, headers, rows)

    log.info("Finished (working). %d notebook(s) written.", len(results))

# if __name__ == "__main__":
#     main()
