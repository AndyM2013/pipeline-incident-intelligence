"""Column-name rules from the Bronze pipeline, tested without Spark."""

import ast
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent / "src" / "pipelines" / "bronze.py"


def load_name_helpers():
    tree = ast.parse(SRC.read_text())
    wanted = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in {"snake_case", "unique_names"}]
    wanted += [n for n in tree.body if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "") == "SYSTEM_COLUMNS"]
    wanted.sort(key=lambda n: n.lineno)  # constants before the functions that use them
    module = ast.fix_missing_locations(
        ast.Module(body=[ast.Import(names=[ast.alias("re")]), *wanted], type_ignores=[])
    )
    ns: dict = {}
    exec(compile(module, str(SRC), "exec"), ns)
    return ns["snake_case"], ns["unique_names"]


snake_case, unique_names = load_name_helpers()


def test_headers_with_spaces_and_punctuation():
    assert snake_case("Incident Number") == "incident_number"
    assert snake_case("Approximate Volume Released (m³)") == "approximate_volume_released_m"
    assert snake_case("  What Happened? ") == "what_happened"


def test_leading_digit_and_empty_names_stay_valid():
    assert snake_case("2nd Cause") == "c_2nd_cause"
    assert snake_case("###") == "col"


def test_collisions_get_suffixes():
    assert unique_names(["Cause", "cause", "CAUSE!"]) == ["cause", "cause_2", "cause_3"]


def test_auto_loader_system_columns_keep_their_names():
    # Regression: snake_case stripped the leading underscore and broke the `_rescued_data IS NULL` expectation.
    assert unique_names(["Incident Number", "_rescued_data"]) == ["incident_number", "_rescued_data"]
