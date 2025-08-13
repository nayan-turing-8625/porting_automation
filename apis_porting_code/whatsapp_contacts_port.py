
whatsapp_contacts_port  = """
import contacts
import google_calendar
import whatsapp
import json
import uuid
from datetime import datetime

def port_db_whatsapp_and_contacts(port_contact_db,port_whatsapp_db) -> None:
    # ================================
    # WHATSAPP DATA CONVERSION
    # ================================
    def convert_whatsapp_contacts(contacts_data, current_user_jid):
        "Convert old WhatsApp contacts format to new v0.1.0 format."
        converted_contacts = {}

        for jid, contact in contacts_data.items():
            jid_full = f"{jid}@s.whatsapp.net"

            # Parse name components
            names = []
            if contact.get("name_in_address_book"):
                parts = contact["name_in_address_book"].split()
                given = parts[0]
                family = " ".join(parts[1:]) if len(parts) > 1 else ""
                names.append({"givenName": given, "familyName": family})

            # Parse phone numbers
            phone_numbers = []
            if contact.get("phone_number"):
                phone_numbers.append({
                    "value": contact["phone_number"],
                    "type": "mobile",
                    "primary": True
                })

            # Create new contact entry
            contact_entry = {
                "resourceName": f"people/{jid_full}",
                "etag": f"etag_{jid}",
                "names": names,
                "emailAddresses": [],
                "phoneNumbers": phone_numbers,
                "organizations": [],
                "whatsapp": {
                    "jid": jid_full,
                    "name_in_address_book": contact.get("name_in_address_book", ""),
                    "profile_name": contact.get("profile_name", ""),
                    "phone_number": contact.get("phone_number", ""),
                    "is_whatsapp_user": contact.get("is_whatsapp_user", False)
                }
            }

            converted_contacts[f"people/{jid_full}"] = contact_entry

        return converted_contacts

    def convert_whatsapp_chats(chats_data, current_user_jid):
        "Convert old WhatsApp chats format to new v0.1.0 format."
        converted_chats = {}

        for chat_id, chat in chats_data.items():
            jid_full = f"{chat_id}@s.whatsapp.net" if "@" not in chat_id else chat_id

            # Convert messages
            messages = []
            for msg in chat["messages"]:
                converted_msg = {
                    "message_id": msg["message_id"],
                    "chat_jid": jid_full,
                    "sender_jid": f"{msg['sender_jid']}@s.whatsapp.net",
                    "sender_name": msg["sender_name"],
                    "timestamp": msg["timestamp"],
                    "text_content": msg["text_content"],
                    "is_outgoing": msg["sender_jid"] == current_user_jid
                }

                # Handle quoted messages if present
                if "quoted_message_info" in msg:
                    converted_msg["quoted_message_info"] = {
                        "quoted_message_id": msg["quoted_message_info"]["quoted_message_id"],
                        "quoted_sender_jid": f"{msg['quoted_message_info']['quoted_sender_jid']}@s.whatsapp.net",
                        "quoted_text_preview": msg["quoted_message_info"]["quoted_text_preview"]
                    }

                messages.append(converted_msg)

            # Calculate last active timestamp
            last_active_timestamp = None
            if messages:
                try:
                    last_ts = max(datetime.fromisoformat(m["timestamp"]) for m in chat["messages"])
                    last_active_timestamp = last_ts.isoformat()
                except Exception:
                    pass

            # Create new chat entry
            new_chat = {
                "chat_jid": jid_full,
                "name": chat.get("name", ""),
                "is_group": chat.get("is_group", False),
                "last_active_timestamp": last_active_timestamp,
                "unread_count": 0,
                "is_archived": chat.get("is_archived", False),
                "is_pinned": chat.get("is_pinned", False),
                "is_muted_until": chat.get("is_muted_until", ""),
                "group_metadata": None,
                "messages": messages
            }

            converted_chats[jid_full] = new_chat

        return converted_chats

    def parse_whatsapp_data(whatsapp_data):
        "Main function to parse old WhatsApp data to new format."
        current_user_jid = f"{whatsapp_data['current_user_jid']}@s.whatsapp.net"

        contacts = convert_whatsapp_contacts(whatsapp_data.get("contacts", {}), whatsapp_data.get('current_user_jid', {}))
        chats = convert_whatsapp_chats(whatsapp_data.get("chats",{}), whatsapp_data.get('current_user_jid', {}))

        return current_user_jid, contacts, chats

    # ================================
    # CONTACTS DATA CONVERSION
    # ================================

    def merge_whatsapp_contacts(whatsapp_contacts, contacts):
        "Add WhatsApp contact data to existing contacts."
        for resource_name, whatsapp_contact in whatsapp_contacts.items():
            contacts[resource_name] = {
                "resourceName": whatsapp_contact["resourceName"],
                "etag": whatsapp_contact["etag"],
                "names": whatsapp_contact["names"],
                "emailAddresses": whatsapp_contact["emailAddresses"],
                "phoneNumbers": whatsapp_contact["phoneNumbers"],
                "organizations": whatsapp_contact["organizations"],
                "addresses": whatsapp_contact.get("addresses", {}),
                "whatsapp": whatsapp_contact["whatsapp"]
            }
        return contacts

    def parse_contacts_data(contacts_data, whatsapp_contacts):
        "Convert old contacts format to new v0.1.0 format."
        parsed_contacts = {}

        for _, contact in contacts_data.items():
            contact_uuid = str(uuid.uuid4())
            resource_name = f"people/{contact_uuid}"

            names = contact.get("names", [])
            contact_name = f"{names[0].get('givenName', '')} {names[0].get('familyName', '')}".strip() if names else ""
            phone_number = contact.get("phoneNumbers", [{"value":str(uuid.uuid4())}])[0]["value"]
            # Create phone endpoints for contact
            phone_endpoints = [
                {
                    "endpoint_type": "PHONE_NUMBER",
                    "endpoint_value": phone.get("value", ""),
                    "endpoint_label": phone.get("type", "")
                }
                for phone in contact.get("phoneNumbers", [])
            ]
            # if the names are conflicting for whatsapp and contact , ensure the contact does have a whatsapp linked contact
            updated_contact = {
                "resourceName": resource_name,
                "etag": uuid.uuid4().hex,
                "names": names,
                "emailAddresses": contact.get("emailAddresses", []),
                "phoneNumbers": contact.get("phoneNumbers", []),
                "organizations": contact.get("organizations", []),
                "addresses": contact.get("addresses", []) or [],
                "notes": contact.get("notes", ""),
                "phone": {
                    "contact_id": contact_uuid,
                    "contact_name": contact_name,
                    "contact_photo_url": None,
                    "contact_endpoints": phone_endpoints
                },
                "whatsapp": {
                  "jid": f"{phone_number}@s.whatsapp.net",
                  "name_in_address_book": contact_name,
                  "profile_name":contact_name,
                  "phone_number": phone_number,
                  "is_whatsapp_user": False
              }
            }
            # there is a conflicting name , so we create the link
            parsed_contacts[resource_name] = updated_contact
        # Merge with WhatsApp contacts
        return merge_whatsapp_contacts(whatsapp_contacts, parsed_contacts)

    # Parse JSON data
    whatsapp_data = json.loads(port_whatsapp_db)
    contact_data = json.loads(port_contact_db)
    # Convert WhatsApp data
    current_user_jid, parsed_whatsapp_contacts, parsed_whatsapp_chats = parse_whatsapp_data(whatsapp_data)

    # Convert contacts data
    parsed_contacts = parse_contacts_data(contact_data, parsed_whatsapp_contacts)

    # Update WhatsApp database
    whatsapp.SimulationEngine.db.DB["current_user_jid"] = current_user_jid
    whatsapp.SimulationEngine.db.DB["contacts"] = parsed_whatsapp_contacts
    whatsapp.SimulationEngine.db.DB["chats"] = parsed_whatsapp_chats

    # Update contacts database
    contacts.SimulationEngine.db.DB["myContacts"] = parsed_contacts

    # Save and reload databases
    contacts.SimulationEngine.db.save_state("/content/DBs/portal_db_contacts.json")
    contacts.SimulationEngine.db.load_state("/content/DBs/portal_db_contacts.json")
    whatsapp.SimulationEngine.db.save_state("/content/DBs/portal_db_whatsapp.json")
    whatsapp.SimulationEngine.db.load_state("/content/DBs/portal_db_whatsapp.json")
"""