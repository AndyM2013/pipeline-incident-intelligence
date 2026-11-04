"""Gold constants (units, phases, regions), tested without Spark."""

import ast
from decimal import Decimal
from pathlib import Path

GOLD = Path(__file__).resolve().parent.parent / "src" / "pipelines" / "gold.py"
tree = ast.parse(GOLD.read_text())
ns = {
    n.targets[0].id: ast.literal_eval(n.value)
    for n in tree.body
    if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "").isupper()
}


def test_barrel_is_exactly_42_us_gallons():
    assert Decimal(ns["BARREL_M3"]) == 42 * Decimal("0.003785411784")


def test_every_profiled_cer_substance_has_exactly_one_phase():
    lists = [ns["CER_GAS"], ns["CER_LIQUID"], ns["CER_PHASE_UNKNOWN"]]
    every = [s for lst in lists for s in lst]
    assert len(every) == len(set(every)) == 31  # 31 substances in current CER incidents, 2026-11-04


def test_hvls_are_phase_unknown():
    assert {"Propane", "Butane", "Natural Gas Liquids", "Mixed HVP Hydrocarbons"} <= set(ns["CER_PHASE_UNKNOWN"])


def test_regions_are_iso_subdivisions():
    codes = list(ns["CER_REGIONS"].values())
    assert len(codes) == len(set(codes))
    assert all(country in {"CA", "US"} and len(code) == 2 for country, code in codes)
    assert len([c for c, _ in codes if c == "CA"]) == 13  # 10 provinces, 3 territories
