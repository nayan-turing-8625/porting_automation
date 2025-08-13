generic_reminders_port = """
def port_generic_reminder_db(source_json_str) -> None:
    # Merge vendor data into GenericReminders default schema and persist
    with open("/content/DBs/GenericRemindersDefaultDB.json") as f:
        default_db = json.load(f)

    src = json.loads(source_json_str, strict=False) if isinstance(source_json_str, str) else (source_json_str or {})
    if not isinstance(src, dict):
        src = {}

    # Merge supported sections
    for key in ("reminders", "operations", "counters", "actions"):
        if key in src:
            default_db[key] = src.get(key, default_db.get(key, [] if key=="reminders" else {}))

    # Push into engine and save
    generic_reminders.SimulationEngine.db.DB.update(default_db)
    out_path = "/content/DBs/GenericRemindersPortedDB.json"
    generic_reminders.SimulationEngine.db.save_state(out_path)
    generic_reminders.SimulationEngine.db.load_state(out_path)
"""
