gmail_port = """
def port_gmail_db(source_json_str) -> None:
    # Load default DB
    with open("/content/DBs/GmailDefaultDB.json") as f:
        defaultdb = json.load(f)

    # Parse source JSON
    source_db = json.loads(source_json_str, strict=False)

    # Initialize user data
    defaultdb['users'] = {'me': {}}
    me = defaultdb['users']['me']

    keys = ["profile", "drafts", "messages", "threads", "labels", "settings", "history", "watch"]
    for k in keys:
        me[k] = source_db.get(k, {})

    defaultdb['attachments'] = source_db.get('attachments', {})

    # Handle settings defaults if not provided
    if 'settings' not in source_db:
        email = me['profile'].get('emailAddress', 'unknown@example.com')
        default_settings = me.get('settings', {
            "imap": {"enabled": True, "server": "imap.gmail.com", "port": 993},
            "pop": {"enabled": False, "server": "pop.gmail.com", "port": 995},
            "vacation": {"enableAutoReply": False, "responseBodyPlainText": ""},
            "language": {"displayLanguage": "en-US"},
            "autoForwarding": {"enabled": False},
            "sendAs": {}
        })
        default_settings['sendAs'] = {
            email: {
                "sendAsEmail": email,
                "displayName": email.split('@')[0].title(),
                "replyToAddress": email,
                "signature": "Regards,\n" + email.split('@')[0].title(),
                "verificationStatus": "accepted",
                "smimeInfo": {
                    "smime_mock_1": {
                        "id": "smime_mock_1",
                        "encryptedKey": "mock_encrypted_key",
                        "default": True
                    }
                }
            }
        }
        me['settings'] = default_settings

    # Update counters
    counters = defaultdb.get("counters", {})
    counters.update({
        "message": len(me.get("messages", {})),
        "thread": len(me.get("threads", {})),
        "draft": len(me.get("drafts", {})),
        "label": len(me.get("labels", {})),
        "history": len(me.get("history", [])),
        "attachment": len(defaultdb.get("attachments", {})),
        "smime": sum(len(info.get("smimeInfo", {})) for info in me.get("settings", {}).get("sendAs", {}).values())
    })
    defaultdb["counters"] = counters

    # Save output DB
    with open("/content/DBs/ported_db_gmail.json", "w") as f:
        json.dump(defaultdb, f, indent=2)
"""