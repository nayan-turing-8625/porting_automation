# generator_drive.py
from __future__ import annotations

import os
from typing import Dict, Any, List, Tuple

import nbformat
from nbformat.v4 import new_notebook, new_markdown_cell

from generator_utils import (
    log, CODEBASE_ROOT, CODEBASE_FOLDER_NAME,
    DEFAULT_DB_PATHS, SERVICE_SPECS, REQUIRED_INPUTS,
    mount_and_import_codebase, auth_services,
    resolve_output_folder_id, empty_drive_folder, upload_notebook_to_drive,
    read_sheet_as_dicts, get_first_sheet_title,
    build_service_code_map_with_logs, services_from_initial_db_columns,
    build_setup_cells, build_import_and_port_cell, build_empty_block, build_warnings_cell,
    upsert_summary_sheet, _now_pacific
)

# ----------- ENV -----------
SPREADSHEET_ID     = os.environ.get("SPREADSHEET_ID", "").strip()                 # common
SOURCE_SHEET_NAME  = os.environ.get("SOURCE_SHEET_NAME", "Template Colab").strip()
CODE_SHEET_NAME    = os.environ.get("CODE_SHEET_NAME", "Translate_JSONs").strip()
SUMMARY_SHEET_NAME = os.environ.get("SUMMARY_SHEET_NAME", "Automated_Generated_Colabs").strip()

# output folder can be an ID or a subfolder name
OUT_HINT           = os.environ.get("TEMPLATE_OUT_FOLDER_NAME", "generated_colabs").strip()

# ----------- NOTEBOOK BUILDERS -----------
def build_metadata_cell(task_id: str, api_modules: List[str]):
    md = [
        f"**Sample ID**: {task_id}\n\n",
        "**Query**:\n\n",                       # (left blank in template flow)
        "**DB Type**: Base Case\n\n",
        "**Case Description**:\n\n",
        "**Global/Context Variables:**\n\n",
        "**APIs:**\n",
    ]
    md += [f"- {a}\n" for a in api_modules]
    md.append("\n**Databases:**")
    return new_markdown_cell("".join(md))

def generate_notebook_for_row(
    row: Dict[str, str],
    idx: int,
    setup_cell: str,
    pipinstall_cell: str,
    code_map: Dict[str, str],
    meta_map: Dict[str, Tuple[str, str]],
) -> Tuple[nbformat.NotebookNode, Dict[str, Any]]:
    # services by *_initial_db presence
    selected = services_from_initial_db_columns(row)

    # build api_modules including dependencies
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

    # issues block (validation)
    issues = {"unknown_services": [s for s in expanded if s not in SERVICE_SPECS], "missing_inputs": [], "json_errors": {}}
    # validate required inputs
    needed = sorted({c for s in expanded for c in REQUIRED_INPUTS.get(s, [])})
    import json as _json
    for col in needed:
        v = row.get(col, "")
        if not str(v).strip():
            issues["missing_inputs"].append(col)
        else:
            try: _json.loads(str(v))
            except Exception as e: issues["json_errors"][col] = str(e)

    nb = new_notebook()
    task_id = (row.get("task_id") or f"row-{idx}").strip() or f"row-{idx}"

    nb.cells.append(build_metadata_cell(task_id, api_modules))
    w = build_warnings_cell(issues)
    if w: nb.cells.append(w)

    nb.cells.append(new_markdown_cell("# Set Up"))
    nb.cells.extend(build_setup_cells(setup_cell, pipinstall_cell))

    nb.cells.append(new_markdown_cell("## Import APIs and initiate DBs"))
    nb.cells.append(build_import_and_port_cell(api_modules, expanded, row, code_map, meta_map))

    # Empty scaffold blocks
    nb.cells.extend(build_empty_block("Initial Assertion"))
    nb.cells.extend(build_empty_block("Action"))
    nb.cells.extend(build_empty_block("Final Assertion"))

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
    # empty_drive_folder(drive, out_folder_id)

    src_name = SOURCE_SHEET_NAME or get_first_sheet_title(sheets, SPREADSHEET_ID)
    log.info("Reading tasks from sheet '%s'", src_name)

    headers, rows = read_sheet_as_dicts(sheets, SPREADSHEET_ID, src_name)
    if not rows:
        log.warning("No rows in source sheet.")
        return
    log.info("Loaded %d tasks.", len(rows))

    # live code
    code_map, meta_map = build_service_code_map_with_logs(sheets, SPREADSHEET_ID, CODE_SHEET_NAME)

    # generate & upload
    results: List[List[str]] = []
    for i, row in enumerate(rows, start=1):
        task_id = (row.get("task_id") or f"row-{i}").strip() or f"row-{i}"
        sel = services_from_initial_db_columns(row)
        log.info("---- Generating (template) for task_id=%s ----", task_id)
        log.info("Selected services: %s", " | ".join(sel) if sel else "(none)")

        nb, issues = generate_notebook_for_row(row, i, setup_cell, pipinstall_cell, code_map, meta_map)
        fname = f"Gemini_Apps_ID_Data_Port_{task_id}.ipynb"
        _, colab_url = upload_notebook_to_drive(drive, out_folder_id, fname, nb)
        log.info("Uploaded: %s", fname)
        log.info("Colab URL: %s", colab_url)
        results.append([task_id, colab_url])

        # warn if validation issues
        if any(issues.values()):
            if issues["unknown_services"]: log.warning("Unknown services for %s: %s", task_id, ", ".join(issues["unknown_services"]))
            if issues["missing_inputs"]:   log.warning("Missing inputs for %s: %s", task_id, ", ".join(issues["missing_inputs"]))
            if issues["json_errors"]:
                for col, err in issues["json_errors"].items():
                    log.warning("JSON error in %s for %s: %s", col, task_id, err)

    # summary: task_id, colab_url, refresh_date, refresh_time
    refresh_date, refresh_time = _now_pacific()
    headers = ["task_id", "colab_url", "refresh_date", "refresh_time"]
    rows = [r + [refresh_date, refresh_time] for r in results]
    upsert_summary_sheet(sheets, SPREADSHEET_ID, SUMMARY_SHEET_NAME, headers, rows)

    log.info("Finished. %d notebook(s) written.", len(results))

# if __name__ == "__main__":
#     main()
