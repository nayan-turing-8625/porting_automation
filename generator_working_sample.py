# generator_working_sample.py
# Working Sheet → Colab notebooks in Drive
# - Pulls *_initial_db inputs from Template sheet (matched by task_id)
# - Query (one line) + Final Assertion from Working sheet
# - Import/DB cell identical to template flow
# - Summary: task_id, query_order, colab_url, refresh_date, refresh_time (PST)

from __future__ import annotations

import os
import re
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
    upsert_summary_sheet, _now_pacific,
    normalize_service_token, reescape_newlines_inside_string_literals,
)

# ---------------- ENV ----------------
SPREADSHEET_ID  = os.environ.get("SPREADSHEET_ID", "").strip()  # common spreadsheet
# Template sheet (source of *_initial_db columns)
TEMPLATE_SHEET  = os.environ.get("SOURCE_SHEET_NAME", "Template Colab").strip()
# Working sheet (source of query/final_assertion_code/query_order)
WORKING_SHEET   = os.environ.get("SOURCE_WORKING_SHEET_NAME", "Working_Sheet_Test_Automation").strip()

CODE_SHEET_NAME = os.environ.get("CODE_SHEET_NAME", "Translate_JSONs").strip()

SUMMARY_WS_NAME = os.environ.get("SUMMARY_SHEET_NAME_WORKING_AUTOMATION", "Automated_Generated_Colabs").strip()
OUT_HINT        = os.environ.get("WS_OUT_FOLDER_NAME", "generated_colabs_working").strip()

# Build the set of ALL initial-db column names we might need to copy from Template
INITIAL_DB_COLS: List[str] = sorted({col for cols in REQUIRED_INPUTS.values() for col in cols})


# --------------- Helpers ---------------

def _fallback_services_from_row(row: Dict[str, str]) -> List[str]:
    """
    If no *_initial_db inputs are found after merging, fall back to a services list
    present in the Working row (services_needed/services_required/services).
    """
    raw = (row.get("services_needed")
           or row.get("services_required")
           or row.get("services")
           or "").strip()
    if not raw:
        return []
    toks = [t.strip() for t in re.split(r"[|,]", raw) if t.strip()]
    out, seen = [], set()
    for t in toks:
        norm = normalize_service_token(t)
        if norm in SERVICE_SPECS and norm not in seen:
            out.append(norm); seen.add(norm)
    return out


def _index_template_rows_by_task(sheets, spreadsheet_id: str, sheet_name: str) -> Dict[str, Dict[str, str]]:
    """
    Read the Template sheet once and index by task_id → row dict.
    """
    _, trows = read_sheet_as_dicts(sheets, spreadsheet_id, sheet_name)
    idx: Dict[str, Dict[str, str]] = {}
    for r in trows:
        tid = (r.get("task_id") or "").strip()
        if tid:
            idx[tid] = r
    return idx


def _merge_working_with_template(working_row: Dict[str, str],
                                 template_row: Dict[str, str] | None) -> Dict[str, str]:
    """
    Copy *_initial_db inputs (and optionally 'services_needed') from template_row into working_row.
    Leaves existing working_row values intact unless we overwrite the initial_db fields.
    """
    merged = dict(working_row)
    if template_row:
        # copy all initial DB columns we care about
        for col in INITIAL_DB_COLS:
            merged[col] = template_row.get(col, "")
        # also copy services_needed to aid summary/fallback (doesn't override if WS already has it)
        if "services_needed" not in merged or not str(merged["services_needed"]).strip():
            merged["services_needed"] = template_row.get("services_needed", "")
    return merged


# --------------- Notebook-specific blocks ---------------

def build_metadata_cell(task_id: str, api_modules: List[str], query_text: str):
    # Query must remain on a single line
    md = [
        f"**Sample ID**: {task_id}\n\n",
        f"**Query**: {query_text}\n\n",
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
        code = reescape_newlines_inside_string_literals(code).strip() + "\n"
    return [new_markdown_cell("# Final Assertion"), new_code_cell(code)]


def generate_notebook_for_row(
    merged_row: Dict[str, str],
    idx: int,
    setup_cell: str,
    pipinstall_cell: str,
    code_map: Dict[str, str],
    meta_map: Dict[str, Tuple[str, str]],
) -> Tuple[nbformat.NotebookNode, Dict[str, Any]]:
    """
    merged_row already contains initial DB columns from Template + working fields.
    """
    # 1) Primary selection from *_initial_db presence
    selected = services_from_initial_db_columns(merged_row)

    # 2) Fallback to services list if still nothing
    if not selected:
        fb = _fallback_services_from_row(merged_row)
        if fb:
            log.info("No *_initial_db found; falling back to services list: %s", " | ".join(fb))
            selected = fb

    # 3) Expand deps and build api_modules
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

    # 4) Validate inputs for the services we’ll port
    issues = {"unknown_services": [s for s in expanded if s not in SERVICE_SPECS], "missing_inputs": [], "json_errors": {}}
    import json as _json
    needed = sorted({c for s in expanded for c in REQUIRED_INPUTS.get(s, [])})
    for col in needed:
        v = merged_row.get(col, "")
        if not str(v).strip():
            issues["missing_inputs"].append(col)
        else:
            try: _json.loads(str(v))
            except Exception as e: issues["json_errors"][col] = str(e)

    # 5) Build notebook (metadata with one-line Query)
    task_id = (merged_row.get("task_id") or f"row-{idx}").strip() or f"row-{idx}"
    query_text = (merged_row.get("query") or "").strip()
    nb = new_notebook()

    nb.cells.append(build_metadata_cell(task_id, api_modules, query_text))
    w = build_warnings_cell(issues)
    if w: nb.cells.append(w)

    # Static blocks
    nb.cells.append(new_markdown_cell("# Set Up"))
    nb.cells.extend(build_setup_cells(setup_cell, pipinstall_cell))

    # Import + Default DB loads + live porting code + calls (shared builder)
    nb.cells.append(new_markdown_cell("## Import APIs and initiate DBs"))
    nb.cells.append(build_import_and_port_cell(api_modules, expanded, merged_row, code_map, meta_map))

    # Scaffold + Final Assertion
    nb.cells.extend(build_empty_block("Initial Assertion"))
    nb.cells.extend(build_empty_block("Action"))
    nb.cells.extend(build_final_assertion_block(merged_row))

    nb.metadata["colab"] = {"provenance": []}
    nb.metadata["language_info"] = {"name": "python"}
    return nb, issues


# --------------- MAIN ---------------

def main():
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID is required.")
    if not CODE_SHEET_NAME:
        raise RuntimeError("CODE_SHEET_NAME is required.")

    # Mount + static cells
    setup_cell, pipinstall_cell = mount_and_import_codebase()
    log.info("Mounted Drive and imported static setup cells from %s", CODEBASE_ROOT)

    # Auth
    drive, sheets = auth_services()
    log.info("Authenticated to Google Drive & Sheets APIs.")

    # Output folder (ID or named subfolder) and empty it
    out_folder_id = resolve_output_folder_id(drive, OUT_HINT, CODEBASE_FOLDER_NAME)
    log.info("Output notebooks Drive folder id: %s", out_folder_id)
    empty_drive_folder(drive, out_folder_id)

    # Read Working rows
    ws_name = WORKING_SHEET or get_first_sheet_title(sheets, SPREADSHEET_ID)
    log.info("Reading working rows from sheet '%s'", ws_name)
    _, working_rows = read_sheet_as_dicts(sheets, SPREADSHEET_ID, ws_name)
    if not working_rows:
        log.warning("No rows in working sheet.")
        return
    log.info("Loaded %d working rows.", len(working_rows))

    # Read Template rows and index by task_id (to fetch *_initial_db)
    tmpl_name = TEMPLATE_SHEET or get_first_sheet_title(sheets, SPREADSHEET_ID)
    log.info("Reading template rows from sheet '%s' for initial_db inputs", tmpl_name)
    tmpl_index = _index_template_rows_by_task(sheets, SPREADSHEET_ID, tmpl_name)
    log.info("Indexed %d template rows by task_id.", len(tmpl_index))

    # Live porting code (latest per service) — FIXED: pass spreadsheet_id + code_sheet_name
    code_map, meta_map = build_service_code_map_with_logs(
        sheets, SPREADSHEET_ID, CODE_SHEET_NAME
    )
    log.info("Prepared porting code for %d services.", len(code_map))

    # Generate notebooks
    results: List[List[str]] = []  # task_id, query_order, colab_url

    for i, wrow in enumerate(working_rows, start=1):
        task_id = (wrow.get("task_id") or f"row-{i}").strip() or f"row-{i}"
        query_order = (wrow.get("query_order") or "").strip() or str(i)
        trow = tmpl_index.get(task_id)

        if trow:
            log.info("Merging initial DB inputs from template for task_id=%s", task_id)
        else:
            log.warning("No matching template row for task_id=%s; proceeding without initial_db inputs.", task_id)

        merged_row = _merge_working_with_template(wrow, trow)

        # Log quick preview of which initial columns we actually got
        got_cols = [c for c in INITIAL_DB_COLS if str(merged_row.get(c, "")).strip()]
        if got_cols:
            log.info("Initial DB columns present for %s: %s", task_id, ", ".join(got_cols))
        else:
            log.info("No initial DB columns present for %s after merge.", task_id)

        # Query preview
        log.info("Query: %s", (merged_row.get("query") or "").strip())

        nb, issues = generate_notebook_for_row(merged_row, i, setup_cell, pipinstall_cell, code_map, meta_map)
        fname = f"Gemini_Apps_Data_Port_{task_id}_turn{query_order}.ipynb"
        _, colab_url = upload_notebook_to_drive(drive, out_folder_id, fname, nb)
        log.info("Uploaded: %s", fname)
        log.info("Colab URL: %s", colab_url)
        results.append([task_id, query_order, colab_url])

        if any(issues.values()):
            if issues["unknown_services"]:
                log.warning("Unknown services for %s: %s", task_id, ", ".join(issues["unknown_services"]))
            if issues["missing_inputs"]:
                log.warning("Missing inputs for %s: %s", task_id, ", ".join(issues["missing_inputs"]))
            if issues["json_errors"]:
                for col, err in issues["json_errors"].items():
                    log.warning("JSON error in %s for %s: %s", col, task_id, err)

    # Summary: task_id, query_order, colab_url, refresh_date, refresh_time (PST)
    refresh_date, refresh_time = _now_pacific()
    headers = ["task_id", "query_order", "colab_url", "refresh_date", "refresh_time"]
    rows = [r + [refresh_date, refresh_time] for r in results]
    upsert_summary_sheet(sheets, SPREADSHEET_ID, SUMMARY_WS_NAME, headers, rows)

    log.info("Finished (working). %d notebook(s) written.", len(results))


if __name__ == "__main__":
    main()
