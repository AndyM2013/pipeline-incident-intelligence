import io
import sys
import zipfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import fetch_snapshots as fs  # noqa: E402


def make_zip(members: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in members.items():
            zf.writestr(name, content)
    return buf.getvalue()


def test_wayback_date_comes_from_the_capture_actually_returned():
    url = "https://web.archive.org/web/20240611083015id_/https://www.cer-rec.gc.ca/open/incident/x.csv"
    assert fs.snapshot_date_from_wayback_url(url) == "2024-06-11"


def test_wayback_date_rejects_non_capture_urls():
    with pytest.raises(ValueError):
        fs.snapshot_date_from_wayback_url("https://www.cer-rec.gc.ca/open/incident/x.csv")


def test_zip_split_routes_data_and_dictionary():
    payload = make_zip({"accident_2010_present.txt": b"a\tb\n1\t2\n", "fields.xlsx": b"xx"})
    landed = fs.split_zip("phmsa_liquid", "2026-10-30", payload, "*.txt")
    paths = {item.relative_path for item in landed}
    assert paths == {
        "phmsa_liquid/2026-10-30/accident_2010_present.txt",
        "_dictionaries/phmsa_liquid/2026-10-30/fields.xlsx",
    }


def test_zip_without_data_member_fails_loudly():
    with pytest.raises(ValueError):
        fs.split_zip("phmsa_liquid", "2026-10-30", make_zip({"readme.pdf": b"x"}), "*.txt")


def test_identical_content_is_not_a_new_snapshot():
    manifest: dict = {}
    first = fs.Landed("cer", "2026-10-01", "data.csv", "data", b"same")
    fs.record(manifest, first)
    assert fs.already_recorded(manifest, fs.Landed("cer", "2026-10-30", "data.csv", "data", b"same"))
    assert not fs.already_recorded(manifest, fs.Landed("cer", "2026-10-30", "data.csv", "data", b"changed"))
