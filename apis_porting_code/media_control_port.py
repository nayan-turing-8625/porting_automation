media_control_port = """
def port_media_control_db(source_json_str) -> None:
      # Load default DB
    with open("/content/DBs/MediaControlDefaultDB.json") as f:
        defaultdb = json.load(f)

    # Parse source JSON
    source_db = json.loads(source_json_str, strict=False)
    defaultdb['active_media_player'] = source_db.get('active_media_player')
    defaultdb['media_players'] = source_db.get('media_players', {})

    media_control.SimulationEngine.db.save_state("/content/DBs/ported_db_media.json")
    media_control.SimulationEngine.db.load_state("/content/DBs/ported_db_media.json")
"""