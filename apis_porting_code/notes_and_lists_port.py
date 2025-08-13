notes_and_lists_port = """
from datetime import datetime, timezone
from notes_and_lists.SimulationEngine.utils import update_title_index, update_content_index

def port_notes_and_lists_initial_db(source_json_str: str) -> None:

    def _to_iso_z(ts: str | None) -> str:
        "Normalize 'YYYY-MM-DDTHH:MM:SS' -> 'YYYY-MM-DDTHH:MM:SSZ'."
        if not ts or not isinstance(ts, str):
            return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        if ts.endswith("Z") or "+" in ts:
            return ts
        return f"{ts}Z"

    src: Dict[str, Any] = json.loads(source_json_str)

    # 1) Reset DB to a clean Default‑like shell
    DB = notes_and_lists.DB  # re-exported by the package
    DB.clear()
    DB.update({
        "notes": {},
        "lists": {},
        "operation_log": {},
        "title_index": {},
        "content_index": {},
    })

    # 2) Migrate NOTES (add content_history if missing; normalize timestamps)
    notes_block = src.get("notes", {})
    if isinstance(notes_block, dict):
        for note_key, note in notes_block.items():
            if not isinstance(note, dict):
                continue
            nid = note.get("id", note_key)
            title = note.get("title")
            content = note.get("content", "") or ""
            created_at = _to_iso_z(note.get("created_at"))
            updated_at = _to_iso_z(note.get("updated_at"))
            content_history = note.get("content_history")
            if not isinstance(content_history, list):
                content_history = []

            DB["notes"][nid] = {
                "id": nid,
                "title": title,
                "content": content,
                "created_at": created_at,
                "updated_at": updated_at,
                "content_history": content_history,
            }

    # 3) Migrate LISTS (drop 'completed'; ensure item_history; normalize timestamps)
    lists_block = src.get("lists", {})
    if isinstance(lists_block, dict):
        for list_key, lst in lists_block.items():
            if not isinstance(lst, dict):
                continue
            lid = lst.get("id", list_key)
            title = lst.get("title")
            created_at = _to_iso_z(lst.get("created_at"))
            updated_at = _to_iso_z(lst.get("updated_at"))
            item_history = lst.get("item_history")
            if not isinstance(item_history, dict):
                item_history = {}

            items_dict: Dict[str, Dict[str, Any]] = {}
            raw_items = lst.get("items", {})
            if isinstance(raw_items, dict):
                for item_key, item in raw_items.items():
                    if not isinstance(item, dict):
                        continue
                    iid = item.get("id", item_key)
                    items_dict[iid] = {
                        "id": iid,
                        "content": item.get("content", "") or "",
                        "created_at": _to_iso_z(item.get("created_at")),
                        "updated_at": _to_iso_z(item.get("updated_at")),
                    }
                    # NOTE: 'completed' is intentionally dropped; not present in Default DB

            DB["lists"][lid] = {
                "id": lid,
                "title": title,
                "items": items_dict,
                "created_at": created_at,
                "updated_at": updated_at,
                "item_history": item_history,
            }

    # 4) Rebuild indexes (titles + content keywords)
    # Notes
    for nid, note in DB["notes"].items():
        update_title_index(note.get("title"), nid)
        update_content_index(nid, note.get("content", ""))

    # Lists + list items
    for lid, lst in DB["lists"].items():
        update_title_index(lst.get("title"), lid)
        for item in lst.get("items", {}).values():
            update_content_index(item["id"], item.get("content", ""))

    # 5) Save and reload
    out_path = "/content/DBs/NotesAndListsPorted.json"
    notes_and_lists.SimulationEngine.db.save_state(out_path)
    notes_and_lists.SimulationEngine.db.load_state(out_path)
"""