device_settings_port = """
def port_device_setting_db(source_json_str) -> None:
      # Load default DB
    with open("/content/DBs/DeviceSettingDefaultDB.json") as f:
        defaultdb = json.load(f)

    # Parse source JSON
    source_db = json.loads(source_json_str, strict=False)
    defaultdb['device_settings'] = source_db.get('device_settings',{})
    defaultdb['installed_apps'] = source_db.get('installed_apps', {})
    defaultdb['device_insights'] = source_db.get('device_insights', {})



        # Save output DB
    with open("/content/DBs/ported_db_device_settings.json", "w") as f:
        json.dump(defaultdb, f, indent=2)
"""