"""Export PHMSA's published hazardous liquid incident trend and land it in the Volume, for reconciliation.

The "Pipeline Incident 20 Year Trends" dashboard (All Reported) is an Oracle Analytics page: it needs JavaScript
and refuses stateless export URLs from an anonymous session. So this drives it with a headless browser the way a
person would: open the public page, open the dashboard with the filters in the URL, click the table's own
"Export > CSV", and check the export says the filters were applied before landing it.

Runs on your own machine, like fetch_snapshots.py (needs requirements-browser.txt and `playwright install chromium`).

    python scripts/fetch_phmsa_trends.py                     # national: year x onshore/offshore
    python scripts/fetch_phmsa_trends.py --state TEXAS       # one state (repeatable)
    python scripts/fetch_phmsa_trends.py --all-states        # every state, one export each (~10 s per state, ~9 min)
    python scripts/fetch_phmsa_trends.py --no-upload

Lands under the Volume's landing root, next to the sources but never read by Bronze:
    published/phmsa_trends/<YYYY-MM-DD>/hazardous_liquid_<scope>.csv
The dashboard's "Data as of" date is printed and kept in export_info.json in the same folder.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import re
import sys
from pathlib import Path
from urllib.parse import quote

from fetch_snapshots import LOCAL_LANDING, MANIFEST_FILE, Landed, already_recorded, record, upload

SOURCE = "published/phmsa_trends"
PORTAL = "https://portalpublic.phmsa.dot.gov"
# The public entry point hands out an anonymous session; the dashboard URL alone redirects to a login page.
ENTRY_URL = (
    f"{PORTAL}/PDMPublicReport?url=%2Fanalytics%2Fsaw.dll%3FPortalPages%26PortalPath%3D%2Fshared%2F"
    "PDM%20Public%20Website%2F_portal%2FSC%20Incident%20Trend%26Page%3DAll%20Reported"
)
DASHBOARD = "/shared/PDM Public Website/_portal/SC Incident Trend"
PAGE = "All Reported"
SYSTEM_TYPE_COLUMN = '"PHP - Pipeline System Type"."System Sub Type"'
STATE_COLUMN = '"PHP - Geo Location"."State Name"'
SYSTEM_TYPE = "HAZARDOUS LIQUID"
# State names as the dashboard's State Name prompt lists them (checked 2026-11-04).
STATES = [
    "ALABAMA", "ALASKA", "ARIZONA", "ARKANSAS", "CALIFORNIA", "COLORADO", "CONNECTICUT", "DELAWARE",
    "DISTRICT OF COLUMBIA", "FLORIDA", "GEORGIA", "HAWAII", "IDAHO", "ILLINOIS", "INDIANA", "IOWA", "KANSAS",
    "KENTUCKY", "LOUISIANA", "MAINE", "MARYLAND", "MASSACHUSETTS", "MICHIGAN", "MINNESOTA", "MISSISSIPPI",
    "MISSOURI", "MONTANA", "NEBRASKA", "NEVADA", "NEW HAMPSHIRE", "NEW JERSEY", "NEW MEXICO", "NEW YORK",
    "NORTH CAROLINA", "NORTH DAKOTA", "OHIO", "OKLAHOMA", "OREGON", "PENNSYLVANIA", "PUERTO RICO", "RHODE ISLAND",
    "SOUTH CAROLINA", "SOUTH DAKOTA", "TENNESSEE", "TEXAS", "UTAH", "VERMONT", "VIRGINIA", "WASHINGTON",
    "WEST VIRGINIA", "WISCONSIN", "WYOMING",
]
LOAD_TIMEOUT_MS = 120_000


def dashboard_url(state: str | None = None) -> str:
    """Dashboard page with prompt values passed as Navigate filters (P0 = number of filters, then op/column/value)."""
    filters = [(SYSTEM_TYPE_COLUMN, SYSTEM_TYPE)] + ([(STATE_COLUMN, state)] if state else [])
    params = [("P0", str(len(filters)))]
    for i, (column, value) in enumerate(filters):
        params += [(f"P{3 * i + 1}", "eq"), (f"P{3 * i + 2}", column), (f"P{3 * i + 3}", f'"{value}"')]
    query = "&".join(f"{k}={quote(v)}" for k, v in params)
    return f"{PORTAL}/analytics/saw.dll?PortalPages&PortalPath={quote(DASHBOARD)}&Page={quote(PAGE)}&Action=Navigate&{query}"


def file_name(state: str | None) -> str:
    scope = re.sub(r"[^a-z0-9]+", "_", state.lower()).strip("_") if state else "national"
    return f"hazardous_liquid_{scope}.csv"


def check_export(content: bytes, state: str | None) -> int:
    """Refuse an export whose own label columns say a different filter was applied. Returns the data row count."""
    rows = list(csv.reader(io.StringIO(content.decode("utf-8-sig"))))
    if len(rows) < 2:
        raise ValueError("export has no data rows")
    header, data = rows[0], rows[1:]
    sys_col, state_col = header.index("SYS"), header.index("State Name")
    systems = {r[sys_col].rsplit("[/b]", 1)[-1].strip() for r in data}
    states = {r[state_col].rsplit("[/b]", 1)[-1].strip() for r in data}
    if systems != {SYSTEM_TYPE}:
        raise ValueError(f"export is for system type {systems}, expected {SYSTEM_TYPE}")
    if states != {state or ""}:
        raise ValueError(f"export is for state {states}, expected {state or 'all states'}")
    return len(data)


def export_csv(page, state: str | None) -> bytes:
    """Open the filtered dashboard, wait until its header shows the filters, and download the table's CSV export."""
    page.goto(dashboard_url(state), wait_until="load", timeout=LOAD_TIMEOUT_MS)
    page.wait_for_function(
        """([sys, state]) => document.body.innerText.includes('System Type: ' + sys)
              && document.body.innerText.includes('State: ' + state)""",
        arg=[SYSTEM_TYPE, state or "(All Column Values)"],
        timeout=LOAD_TIMEOUT_MS,
    )
    link = page.wait_for_selector('a[onclick*="Format=csv"]', state="attached", timeout=LOAD_TIMEOUT_MS)
    call = re.search(r"return (Download\(.*\));", link.get_attribute("onclick")).group(1)
    with page.expect_download(timeout=LOAD_TIMEOUT_MS) as download:
        page.evaluate(call)
    return Path(download.value.path()).read_bytes()


def data_as_of(page) -> str | None:
    match = re.search(r"Data as of (\d{1,2}/\d{1,2}/\d{4})", page.inner_text("body"))
    return dt.datetime.strptime(match.group(1), "%m/%d/%Y").date().isoformat() if match else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--state", action="append", type=str.upper, help="Also export one state (repeatable).")
    parser.add_argument("--all-states", action="store_true", help="Also export every state in STATES.")
    parser.add_argument("--catalog", default="pipeline_intel")
    parser.add_argument("--profile", help="Databricks CLI profile (from ~/.databrickscfg) used for uploads.")
    parser.add_argument("--no-upload", action="store_true", help="Write locally only.")
    parser.add_argument("--force", action="store_true", help="Land files even if identical content was seen before.")
    args = parser.parse_args(argv)
    unknown = set(args.state or []) - set(STATES)
    if unknown:
        parser.error(f"unknown state(s): {', '.join(sorted(unknown))}")
    scopes: list[str | None] = [None] + (STATES if args.all_states else args.state or [])

    from playwright.sync_api import sync_playwright  # only needed here; tests import the helpers above

    manifest = json.loads(MANIFEST_FILE.read_text()) if MANIFEST_FILE.exists() else {}
    today = dt.date.today().isoformat()
    exported: dict[str, int] = {}
    landed_count = 0
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_context(accept_downloads=True).new_page()
        page.goto(ENTRY_URL, wait_until="load", timeout=LOAD_TIMEOUT_MS)
        as_of = None
        items = []
        failed: dict[str, str] = {}
        for state in scopes:
            try:  # one slow or empty state must not cost the other 51 exports
                content = export_csv(page, state)
                exported[file_name(state)] = check_export(content, state)
            except Exception as exc:  # noqa: BLE001  (reported below and in export_info.json)
                failed[state or "national"] = f"{type(exc).__name__}: {str(exc).splitlines()[0]}"
                print(f"FAILED {state or 'national'}: {failed[state or 'national']}")
                continue
            as_of = as_of or data_as_of(page)
            items.append(Landed(SOURCE, today, file_name(state), "data", content))
            print(f"export {state or 'national'}: {exported[file_name(state)]} rows")
        browser.close()

    info = {"dashboard": f"{DASHBOARD} / {PAGE}", "system_type": SYSTEM_TYPE, "data_as_of": as_of, "rows": exported}
    if failed:
        info["failed"] = failed
    print(f"dashboard data as of {as_of}")
    items.append(Landed(SOURCE, today, "export_info.json", "data", (json.dumps(info, indent=2) + "\n").encode()))
    for item in items:
        if not args.force and already_recorded(manifest, item):
            print(f"skip   {item.relative_path}  (same content as an earlier snapshot)")
            continue
        local = LOCAL_LANDING / item.relative_path
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(item.content)
        if not args.no_upload:
            upload(local, item.relative_path, args.catalog, args.profile)
        record(manifest, item)
        landed_count += 1
        print(f"landed {item.relative_path}  ({len(item.content):,} bytes)")

    MANIFEST_FILE.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(f"{landed_count} file(s) landed, {len(failed)} export(s) failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
