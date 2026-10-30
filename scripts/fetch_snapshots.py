"""Download dated snapshots of the incident sources and upload them to the landing Volume.

Runs on your own machine (Free Edition notebooks can only reach a short list of domains).

    python scripts/fetch_snapshots.py                         # today's copy of every source
    python scripts/fetch_snapshots.py --source cer            # one source
    python scripts/fetch_snapshots.py --source cer --wayback 20240601
                                                              # a historical copy from the Internet Archive,
                                                              # dated by the capture it actually returns
    python scripts/fetch_snapshots.py --no-upload             # download only, upload later

Layout written locally under data/landing and mirrored to the Volume:
    <source>/<YYYY-MM-DD>/<file>                  data files read by the Bronze pipeline
    _dictionaries/<source>/<YYYY-MM-DD>/<file>    data dictionaries and field descriptions

A file whose content (SHA-256) matches a snapshot already recorded for that source is skipped,
so re-running is safe and an unchanged publication never becomes a fake "new version".
"""

from __future__ import annotations

import argparse
import datetime as dt
import fnmatch
import hashlib
import io
import json
import re
import subprocess
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
SOURCES_FILE = Path(__file__).resolve().parent / "sources.json"
LOCAL_LANDING = REPO_ROOT / "data" / "landing"
MANIFEST_FILE = REPO_ROOT / "data" / "manifest.json"
USER_AGENT = "pipeline-incident-intelligence/0.1 (personal research)"
WAYBACK_TS = re.compile(r"/web/(\d{8})\d*id_/")


@dataclass(frozen=True)
class Landed:
    """One file ready to land: where it goes and what it contains."""

    source: str
    snapshot_date: str  # YYYY-MM-DD
    name: str
    role: str  # "data" or "dictionary"
    content: bytes

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        if self.role == "data":
            return f"{self.source}/{self.snapshot_date}/{self.name}"
        return f"_dictionaries/{self.source}/{self.snapshot_date}/{self.name}"


def snapshot_date_from_wayback_url(final_url: str) -> str:
    """The archive redirects to the nearest capture; its timestamp is the real snapshot date."""
    match = WAYBACK_TS.search(final_url)
    if not match:
        raise ValueError(f"Not a Wayback capture URL: {final_url}")
    return dt.datetime.strptime(match.group(1), "%Y%m%d").date().isoformat()


def split_zip(source: str, snapshot_date: str, payload: bytes, data_glob: str) -> list[Landed]:
    """Data members matching data_glob land as data; everything else as dictionary material."""
    landed = []
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            name = Path(info.filename).name
            role = "data" if fnmatch.fnmatch(name.lower(), data_glob.lower()) else "dictionary"
            landed.append(Landed(source, snapshot_date, name, role, zf.read(info)))
    if not any(item.role == "data" for item in landed):
        raise ValueError(f"{source}: no member matched {data_glob!r} in the zip")
    return landed


def already_recorded(manifest: dict, item: Landed) -> bool:
    seen = manifest.get(item.source, {}).get(item.name, {})
    return item.sha256 in seen.values()


def record(manifest: dict, item: Landed) -> None:
    manifest.setdefault(item.source, {}).setdefault(item.name, {})[item.snapshot_date] = item.sha256


def download(url: str, wayback: str | None) -> tuple[bytes, str | None]:
    """Return (content, snapshot_date or None for today)."""
    target = f"https://web.archive.org/web/{wayback}id_/{url}" if wayback else url
    resp = requests.get(target, headers={"User-Agent": USER_AGENT}, timeout=120)
    resp.raise_for_status()
    date = snapshot_date_from_wayback_url(resp.url) if wayback else None
    return resp.content, date


def collect(source: str, spec: dict, wayback: str | None, today: str) -> list[Landed]:
    landed: list[Landed] = []
    for f in spec["files"]:
        content, archived_date = download(f["url"], wayback)
        date = archived_date or today
        name = Path(f["url"].split("?")[0]).name
        if f["role"] == "zip":
            landed.extend(split_zip(source, date, content, f.get("data_glob", "*.txt")))
        else:
            landed.append(Landed(source, date, name, f["role"], content))
    return landed


def upload(local: Path, relative_path: str, catalog: str) -> None:
    remote = f"dbfs:/Volumes/{catalog}/bronze/landing/{relative_path}"
    subprocess.run(["databricks", "fs", "cp", str(local), remote, "--overwrite"], check=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", action="append", help="Source key from sources.json (repeatable). Default: all.")
    parser.add_argument("--wayback", metavar="YYYYMMDD", help="Fetch the Internet Archive capture nearest this date.")
    parser.add_argument("--catalog", default="pipeline_intel")
    parser.add_argument("--no-upload", action="store_true", help="Write locally only.")
    parser.add_argument("--force", action="store_true", help="Land files even if identical content was seen before.")
    args = parser.parse_args(argv)

    sources = {k: v for k, v in json.loads(SOURCES_FILE.read_text()).items() if not k.startswith("_")}
    wanted = args.source or list(sources)
    unknown = set(wanted) - set(sources)
    if unknown:
        parser.error(f"unknown source(s): {', '.join(sorted(unknown))}")

    manifest = json.loads(MANIFEST_FILE.read_text()) if MANIFEST_FILE.exists() else {}
    today = dt.date.today().isoformat()
    landed_count = 0

    for source in wanted:
        for item in collect(source, sources[source], args.wayback, today):
            if not args.force and already_recorded(manifest, item):
                print(f"skip   {item.relative_path}  (same content as an earlier snapshot)")
                continue
            local = LOCAL_LANDING / item.relative_path
            local.parent.mkdir(parents=True, exist_ok=True)
            local.write_bytes(item.content)
            if not args.no_upload:
                upload(local, item.relative_path, args.catalog)
            record(manifest, item)
            landed_count += 1
            print(f"landed {item.relative_path}  ({len(item.content):,} bytes)")

    MANIFEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_FILE.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(f"{landed_count} file(s) landed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
