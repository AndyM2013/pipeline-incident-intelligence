"""reference/operator_map.csv: the reviewed operator resolution, checked without Spark."""

import ast
import csv
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SILVER = ROOT / "src" / "pipelines" / "silver.py"


def constant(name):
    tree = ast.parse(SILVER.read_text())
    node = next(n for n in tree.body if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "") == name)
    return ast.literal_eval(node.value)


with open(ROOT / "reference" / "operator_map.csv", newline="", encoding="utf-8") as f:
    rows = [{k: (v or None) for k, v in r.items()} for r in csv.DictReader(f)]
cer = [r for r in rows if r["source"] == "cer"]
phmsa = [r for r in rows if r["source"] == "phmsa"]
PARENT_GROUPS = constant("PARENT_GROUPS")


def test_csv_has_the_columns_the_pipeline_reads():
    assert list(rows[0]) == constant("OPERATOR_COLUMNS")
    assert {r["source"] for r in rows} == {"cer", "phmsa"}


def test_every_cer_company_string_is_listed_once():
    # 63 distinct company strings across all 5 CER snapshots (2026-11-04)
    names = [r["source_operator"] for r in cer]
    assert len(names) == len(set(names)) == 63
    assert all(r["operator_key"] and r["operator_name"] for r in cer)


def test_phmsa_rows_keep_the_operator_id_as_key():
    ids = [r["source_operator"] for r in phmsa]
    assert len(ids) == len(set(ids)) and all(i.isdigit() for i in ids)
    assert all(r["operator_key"] == f"phmsa:{r['source_operator']}" for r in phmsa)
    assert all(r["operator_name"] is None for r in phmsa)  # the latest reported name comes from the data


def test_one_key_has_one_name_and_one_parent():
    seen = defaultdict(set)
    for r in rows:
        seen[r["operator_key"]].add((r["operator_name"], r["parent_group"]))
    assert {k: v for k, v in seen.items() if len(v) > 1} == {}


def test_every_parent_group_is_known_and_cross_border():
    by_group = defaultdict(set)
    for r in rows:
        if r["parent_group"]:
            by_group[r["parent_group"]].add(r["source"])
    assert set(by_group) == set(PARENT_GROUPS)
    assert all(sources == {"cer", "phmsa"} for sources in by_group.values())


def test_name_lookalikes_are_in_no_group():
    lookalikes = {"39401", "31570", "39043", "39596", "39131", "40036"}  # Great Salt Plains, Pony Express, ...
    assert all(r["parent_group"] is None for r in phmsa if r["source_operator"] in lookalikes)
    assert next(r for r in cer if r["source_operator"].startswith("NOVA Chemicals"))["operator_key"] != "ngtl"


def test_listed_phmsa_rows_without_a_group_say_why():
    assert all(r["rationale"] for r in phmsa if r["parent_group"] is None)
