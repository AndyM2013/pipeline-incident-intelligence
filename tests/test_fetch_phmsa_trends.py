import sys
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import fetch_phmsa_trends as ft  # noqa: E402


def filters(url: str) -> dict:
    return {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}


def test_national_url_filters_system_type_only():
    q = filters(ft.dashboard_url())
    assert q["Action"] == "Navigate" and q["P0"] == "1"
    assert (q["P1"], q["P2"], q["P3"]) == ("eq", ft.SYSTEM_TYPE_COLUMN, '"HAZARDOUS LIQUID"')


def test_state_url_adds_a_second_filter():
    q = filters(ft.dashboard_url("NEW MEXICO"))
    assert q["P0"] == "2"
    assert (q["P4"], q["P5"], q["P6"]) == ("eq", ft.STATE_COLUMN, '"NEW MEXICO"')


def test_file_names():
    assert ft.file_name(None) == "hazardous_liquid_national.csv"
    assert ft.file_name("DISTRICT OF COLUMBIA") == "hazardous_liquid_district_of_columbia.csv"


def export(sys_label: str, state_label: str, rows: int = 2) -> bytes:
    # Shape of the real export: BOM, label columns carry the applied filters in [b] markup.
    lines = ["﻿Calendar Year,# of Incidents,State Name,SYS"]
    lines += [f"2020,{i},[b] STATE:[/b] {state_label},[b] System Type:[/b] {sys_label}" for i in range(rows)]
    return ("\n".join(lines) + "\n").encode("utf-8")


def test_check_export_accepts_matching_filters():
    assert ft.check_export(export("HAZARDOUS LIQUID", ""), None) == 2
    assert ft.check_export(export("HAZARDOUS LIQUID", "TEXAS", rows=3), "TEXAS") == 3


@pytest.mark.parametrize(
    "content, state",
    [
        (export("GAS TRANSMISSION", ""), None),  # system filter ignored
        (export("HAZARDOUS LIQUID", ""), "TEXAS"),  # state filter ignored: national data under a state name
        (export("HAZARDOUS LIQUID", "OKLAHOMA"), "TEXAS"),
        (export("HAZARDOUS LIQUID", "", rows=0), None),
    ],
)
def test_check_export_refuses_unfiltered_or_empty_exports(content, state):
    with pytest.raises(ValueError):
        ft.check_export(content, state)


def test_states_are_unique_and_upper_case():
    assert len(ft.STATES) == len(set(ft.STATES)) == 52
    assert all(s == s.upper() for s in ft.STATES)
