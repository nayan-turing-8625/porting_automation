# contact_port = """
# def port_db_contacts(port_contact_db)->None:
#     port_contact_db = json.loads(port_contact_db)
#     contacts.SimulationEngine.db.DB['myContacts'] = {}
#     for key, contact in port_contact_db.items():
#         # Generate a unique resource name (use phone if available, else uuid)
#         phone = contact.get('phoneNumbers', [{}])[0].get('value', None)
#         if phone:
#             resource_name = f"people/{phone.replace('+','').replace('-','').replace(' ','')}"
#         else:
#             resource_name = f"people/{uuid.uuid4()}"

#         # Prepare the contact entry
#         entry = {
#             "resourceName": resource_name,
#             "etag": str(uuid.uuid4()),
#             "names": contact.get("names", []),
#             "emailAddresses": contact.get("emailAddresses", []),
#             "phoneNumbers": contact.get("phoneNumbers", []),
#             "organizations": contact.get("organizations", []),
#             "directory": contact.get("directory", [])
#             # Add other fields as needed
#         }
#         # Add to myContacts (overwrites if already exists)
#         contacts.SimulationEngine.db.DB['myContacts'][resource_name] = entry
#     contacts.SimulationEngine.db.DB['otherContacts'] = contact.get("otherContacts", {})
#     contacts.SimulationEngine.db.DB['directory'] = contact.get("directory", {})
#     # Save and reload databases
#     contacts.SimulationEngine.db.save_state("/content/DBs/portal_db_contacts.json")
#     contacts.SimulationEngine.db.load_state("/content/DBs/portal_db_contacts.json")
# """

contact_port = """
def port_db_contacts(port_contact_db)->None:
    data = json.loads(port_contact_db) if isinstance(port_contact_db, str) else (port_contact_db or {})
    if not isinstance(data, dict):
        data = {}

    # Initialize buckets
    contacts.SimulationEngine.db.DB.setdefault('myContacts', {})
    contacts.SimulationEngine.db.DB.setdefault('otherContacts', {})
    contacts.SimulationEngine.db.DB.setdefault('directory', {})

    for key, contact in data.items():
        if not isinstance(contact, dict):
            continue
        # Generate a unique resource name (use phone if available, else uuid)
        phone = (contact.get('phoneNumbers') or [{}])[0].get('value')
        if phone:
            resource_name = f"people/{str(phone).replace('+','').replace('-','').replace(' ','')}"
        else:
            resource_name = f"people/{uuid.uuid4()}"

        entry = {
            "resourceName": resource_name,
            "etag": str(uuid.uuid4()),
            "names": contact.get("names", []),
            "emailAddresses": contact.get("emailAddresses", []),
            "phoneNumbers": contact.get("phoneNumbers", []),
            "organizations": contact.get("organizations", []),
            "addresses": contact.get("addresses", []),
            "notes": contact.get("notes", "")
        }
        contacts.SimulationEngine.db.DB['myContacts'][resource_name] = entry

    # Optional top-level blocks if present
    if isinstance(data.get("otherContacts"), dict):
        contacts.SimulationEngine.db.DB['otherContacts'] = data["otherContacts"]
    if isinstance(data.get("directory"), dict):
        contacts.SimulationEngine.db.DB['directory'] = data["directory"]

    # Save and reload
    contacts.SimulationEngine.db.save_state("/content/DBs/portal_db_contacts.json")
    contacts.SimulationEngine.db.load_state("/content/DBs/portal_db_contacts.json")
"""
