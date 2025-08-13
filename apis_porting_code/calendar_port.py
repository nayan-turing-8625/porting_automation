calendar_port = """
def port_calendar_db(source_json_str) -> None:
    given_json = json.loads(source_json_str)

    # Load the default DB's
    google_calendar.SimulationEngine.db.load_state("/content/DBs/CalendarDefaultDB.json")

    # Get calendars and set primary flag
    calendars = given_json.get("calendars", {})
    for i, (cal_id, cal_data) in enumerate(calendars.items()):
        cal_data["primary"] = (i == 0)

    # Populate DB
    google_calendar.SimulationEngine.db.DB["calendars"] = calendars
    google_calendar.SimulationEngine.db.DB["calendar_list"] = calendars
    google_calendar.SimulationEngine.db.DB["acl_rules"] = given_json.get("acl_rules", {})
    google_calendar.SimulationEngine.db.DB["channels"] = given_json.get("channels", {})
    google_calendar.SimulationEngine.db.DB["colors"] = given_json.get("colors", {})

    # the events here takes a  tuple as key instead : seperated keys
    google_calendar.SimulationEngine.db.DB["events"] = {
        tuple(key.split(":")): value for key, value in given_json.get("events", {}).items()
    }

    # we load the current updated memory db and save in as json file
    google_calendar.SimulationEngine.db.save_state("/content/DBs/ported_db_calendar.json")
    # we load the saved json file as current db to overwrite
    google_calendar.SimulationEngine.db.load_state("/content/DBs/ported_db_calendar.json")

"""