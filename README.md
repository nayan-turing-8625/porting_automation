# Port Automation – Colab Notebook Generator

Generate fully-formed **Google Colab notebooks**—one per task—from a **live Google Sheet tracker**.
Each notebook auto-loads API packages, ports “initial DB” snapshots into the default DBs, and leaves empty **Initial / Action / Final Assertion** blocks for raters.

> **Highlights**
>
> * Drive-native workflow (run from Colab).
> * Dynamic **APIs** section and import/port code per row’s `services_needed`.
> * Supports: **contacts, calendar, gmail, whatsapp, device\_settings, media\_control, clock, reminders, notes**.
> * Writes a **summary tab** with `colab_url` back into the tracker.
> * Strict JSON handling (e.g., `"true"`→`True`), print/newline escaping fixed for Colab cells.
> * Notebook filenames: `Gemini_Apps_ID_Data_Port_<task_id>.ipynb`.

---

## 🚧 What this repo does

For every row in your Google Sheet:

1. Parses `services_needed` (with synonyms & implicit deps).
2. Pulls the service-specific **porting code** from `apis_porting_code/*` (strings), pastes it into the notebook.
3. Loads the correct **Default DBs** for the selected APIs.
4. Injects the row’s **initial DBs** into variables (dict or JSON string—per porter needs).
5. Calls the porter functions to **port** the data into the runtime DBs.
6. Uploads the notebook to Drive folder `port_automation/generated_colabs/`.
7. Appends a `colab_url` column to a **summary sheet** (`Generated_Colabs`) in your tracker.

---

## 📁 Repository layout

```
port_automation/
├─ generator_drive.py                # Main generator (Drive + Sheets)
├─ static_code/
│  ├─ setup_code_cell.py             # setup_cell: downloads APIs zip, extracts, generates schemas
│  └─ pipinstall_cell.py             # pipinstall_cell: pip install requirements
├─ apis_porting_code/
│  ├─ whatsapp_contacts_port.py      # whatsapp_contacts_port (string: defs for porting)
│  ├─ calendar_port.py               # calendar_port
│  ├─ contact_port.py                # contact_port
│  ├─ gmail_port.py                  # gmail_port
│  ├─ device_settings_port.py        # device_settings_port
│  ├─ media_control_port.py          # media_control_port
│  ├─ clock_port.py                  # clock_port
│  ├─ generic_reminders_port.py      # generic_reminders_port
│  └─ notes_and_lists_port.py        # notes_and_lists_port
└─ (optional) docs/, examples/, etc.
```

---

## ✅ Requirements

* Run in **Google Colab**.
* Your repo lives at: `My Drive/port_automation/` (mount path: `/content/drive/MyDrive/port_automation`).
* A **Google Sheet tracker** with at minimum:

  * `task_id`
  * `services_needed` (e.g., `Calendar | WhatsApp`)
  * One or more “initial DB” JSON columns, e.g.:

    * `contacts_initial_db`, `calendar_initial_db`, `whatsapp_initial_db`, `gmail_initial_db`,
      `device_settings_initial_db`, `media_control_initial_db`, `clock_initial_db`,
      `reminders_initial_db`, `notes_initial_db`

> The JSON in those cells should be valid JSON (`true/false/null` in lower-case). The generator parses them strictly.

---

## 🛠️ First-time setup (Colab)

1. **Mount Drive and add repo to import path**

```python
from google.colab import drive
drive.mount('/content/drive')

import sys
sys.path.append("/content/drive/MyDrive/port_automation")
```

2. **Set environment variables**

```python
import os
os.environ["SPREADSHEET_ID"] = "<YOUR_TRACKER_SHEET_ID>"  # required
# optional:
# os.environ["SOURCE_SHEET_NAME"]  = "Sheet1"
# os.environ["SUMMARY_SHEET_NAME"] = "Generated_Colabs"
# os.environ["OUT_FOLDER_NAME"]    = "generated_colabs"   # under port_automation/
```

3. **Run the generator**

```python
import generator_drive
generator_drive.main()
```

* Notebooks are uploaded to: `My Drive/port_automation/generated_colabs/`
* The tracker gets a new or refreshed tab: **Generated\_Colabs**, same columns + a `colab_url` at the end.

---

## 🧠 Service mapping & defaults

### Tracker token → API package (+ implicit dependencies)

| Tracker token     | API package         | Implicit deps |
| ----------------- | ------------------- | ------------- |
| `whatsapp`        | `whatsapp`          | `contacts`    |
| `contacts`        | `contacts`          | —             |
| `calendar`        | `google_calendar`   | —             |
| `gmail`           | `gmail`             | —             |
| `device_settings` | `device_setting`¹   | —             |
| `media_control`   | `media_control`     | —             |
| `clock`           | `clock`             | —             |
| `reminders`       | `generic_reminders` | —             |
| `notes`           | `notes_and_lists`   | —             |

> ¹ Package is singular (`device_setting`), even though the tracker says `device_settings`.

**Synonyms handled** automatically in `services_needed`, e.g.:

* `Google Calendar`, `Calender` → `calendar`
* `Message`, `WhatsApp messages` → `whatsapp`
* `Generic Reminders`, `Reminder` → `reminders`
* `Notes and Lists`, `notes_and_lists` → `notes`

### Default DB load paths (inside generated notebooks)

```python
DEFAULT_DB_PATHS = {
  "contacts":           "DBs/ContactsDefaultDB.json",      # relative per template
  "whatsapp":           "/content/DBs/WhatsAppDefaultDB.json",
  "google_calendar":    "/content/DBs/CalendarDefaultDB.json",
  "gmail":              "/content/DBs/GmailDefaultDB.json",
  "device_setting":     "/content/DBs/DeviceSettingDefaultDB.json",
  "media_control":      "/content/DBs/MediaControlDefaultDB.json",
  "clock":              "/content/DBs/ClockDefaultDB.json",
  "generic_reminders":  "/content/DBs/GenericRemindersDefaultDB.json",
  "notes_and_lists":    "/content/DBs/NotesAndListsDefaultDB.json",
}
```

> **Notes**: when `notes_and_lists` is used, the generator also injects:

```python
from notes_and_lists.SimulationEngine.utils import update_title_index, update_content_index
from typing import Dict, Any
from datetime import timezone
```

---

## 📓 Notebook structure

Each generated notebook contains:

1. **Meta Data Section** (Markdown)

   * Sample ID, Query (blank), DB Type, Case Description (blank), Global/Context Variables (blank)
   * **APIs** list (dynamic), **Databases** (header only)

2. **Set Up**

   * **Download relevant files**: `static_code/setup_code_cell.setup_cell`
     (Authenticates, downloads `APIs_V0.1.0.zip` from Drive folder `1QpkAZxXhVFzIbm8qPGPRP1YqXEvJ4uD4`, extracts `APIs/`, `DBs/`, `Scripts/`, generates schemas under `/content/Schemas`)

   * **Install Dependencies and Clone Repositories**: `static_code/pipinstall_cell.pipinstall_cell`
     (`!pip install -r /content/APIs/requirements.txt`)

   > The generator re-escapes string literals so your `print("...")` statements never break the cell.

3. **Import APIs and initiate DBs** (Code)

   * `import` API packages based on `services_needed`
   * **Load default DBs** (paths above)
   * **Inject initial DBs** from the sheet into variables:

     * *As dict* or *as JSON string* depending on the porter function
   * **Paste** the porter code block (from `apis_porting_code/...`) and call it.

   **Important quirk**

   * **Clock** porter expects a **JSON string** → generator writes `clock_src_json = json.dumps({...})` and calls `port_clock_db(clock_src_json)`.
   * **Calendar** porter also takes a JSON string → generator writes `port_calender_db` as a **dict** then calls `port_calendar_db(json.dumps(port_calender_db))`.

4. **Initial Assertion / Action / Final Assertion** (empty code cells)

---

## 🔐 Static setup cell (what it does)

`static_code/setup_code_cell.py` runs inside the notebook and:

* Authenticates user
* Finds and downloads **`APIs_V0.1.0.zip`** from Drive folder **`1QpkAZxXhVFzIbm8qPGPRP1YqXEvJ4uD4`**
* Extracts `APIs/`, `DBs/`, `Scripts/` to `/content`
* Adds `/content/APIs` to `sys.path`
* Generates **FC schemas** under `/content/Schemas`
* Prints verification lines (with safe `print("...")` formatting)

> If you rev versions or move the Drive folder, update this module.

---

## ▶️ Running

From Colab:

```python
from google.colab import drive; drive.mount('/content/drive')
import sys, os
sys.path.append("/content/drive/MyDrive/port_automation")

os.environ["SPREADSHEET_ID"] = "<YOUR_SHEET_ID>"
# os.environ["SOURCE_SHEET_NAME"]  = "Sheet1"         # optional
# os.environ["SUMMARY_SHEET_NAME"] = "Generated_Colabs"
# os.environ["OUT_FOLDER_NAME"]    = "generated_colabs"

import generator_drive
generator_drive.main()
```

**Outputs**

* Notebooks → `My Drive/port_automation/generated_colabs/`
* Summary tab → `Generated_Colabs` (in your tracker)

---

## 🧾 Optional: 3-column runtime summary (separate utility)

If you prefer a standalone summary with only `task_id, services_required, colab_url` saved to a **custom folder**, use the provided “runtime summary” cell (from our earlier conversation). It:

* Scans a chosen Drive folder for generated notebooks
* Builds the 3-column CSV
* Creates a **new** Google Sheet in any Drive path you specify (no edits to your tracker)

> Ask if you want this included as a `utils/summary_builder.py` script.

---

## 🧩 Extending to new APIs

To add a new service:

1. Create `apis_porting_code/<new_service>_port.py`

   * Export a **string variable** with the full code block (e.g., `my_service_port = """ ... """`)
   * The code should define a `port_<...>_db(...)` function your notebook will call.

2. Update `generator_drive.py`

   * `SERVICE_SPECS`: add tracker token → package name (and any implicit deps)
   * `DEFAULT_DB_PATHS`: add default DB path for the package (if applicable)
   * `REQUIRED_INPUTS`: list the `*_initial_db` columns needed
   * `PORTING_SPECS`: add:

     * `import_path`, `code_var`
     * `json_vars`: list of `(column_name, injected_var_name, inject_as_dict_bool)`
     * `pre_call_lines` (optional)
     * `call`: function invocation string (using injected var names)

3. If the porter requires extra imports in the notebook, add them to `build_import_and_port_cell` similarly to how `notes_and_lists` imports `update_title_index` helpers.

---

## 🧪 Input expectations & parsing

* Initial DB cells must contain **valid JSON**. The generator uses `json.loads` (strict):

  * `"true"/"false"` → Python `True`/`False`
  * `"null"` → `None`
* The generator normalizes service tokens (`"Google Calendar"`, `"Calender"`, `"WhatsApp messages"`, `"Generic Reminders"`, etc.)
* **WhatsApp** cases implicitly include **contacts** even if not listed.

---

## 🧯 Troubleshooting

**❌ `Folder 'port_automation' not found in My Drive root`**
Make sure the repo is at: `My Drive/port_automation/`.

**❌ `No data found in source sheet`**
Check `SPREADSHEET_ID` and (optionally) `SOURCE_SHEET_NAME`.

**❌ JSON parse error in initial DB**
The cell must be valid JSON. Fix quoting, trailing commas, or convert Python booleans to JSON (`true/false`).

**❌ Missing required inputs**
For a service, required `*_initial_db` columns must be present (see **REQUIRED\_INPUTS**).
WhatsApp needs **`whatsapp_initial_db` + `contacts_initial_db`**.

**❌ Unknown/unsupported service**
The tracker cell contains a token not mapped in **SERVICE\_SPECS**. Add a synonym, or correct the value.

**❌ Newlines breaking `print(...)`**
We re-escape only *inside* string literals while preserving `print("...")`. If you see issues in a pasted block, check your porting code string for unbalanced quotes.

**❌ Clock TypeError (dict vs string)**
Fixed: **Clock** porter now receives a JSON string (`clock_src_json`) so internal `json.loads(...)` works.

---

## 🔒 Notes & Reminders name quirks

* Tracker `reminders` maps to **`generic_reminders`** package.
* Tracker `notes` maps to **`notes_and_lists`** package and imports:

  * `from notes_and_lists.SimulationEngine.utils import update_title_index, update_content_index`

---

## 🧾 License

*Add your license here (MIT, Apache-2.0, etc.).*

---

## 🙋 Support

If you want:

* the generator to **write a brand-new summary spreadsheet** (instead of updating the tracker),
* to target a **custom Drive folder** for summaries,
* or a **local-only** mode (no Drive),

ping me and I’ll include the minimal diffs or a dedicated utility script.
Author: [nayan.k@turing.com]
