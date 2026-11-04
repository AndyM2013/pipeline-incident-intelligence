"""Label constants and expectation SQL from the Silver pipeline, tested without Spark."""

import ast
import re
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent / "src" / "pipelines" / "silver.py"


def load_constants():
    tree = ast.parse(SRC.read_text())
    wanted = [n for n in tree.body if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "").isupper()]
    wanted += [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "only_known"]
    wanted.sort(key=lambda n: n.lineno)
    # CER_EXPECTATIONS calls only_known, so define it before evaluating the constants that use it.
    funcs = [n for n in wanted if isinstance(n, ast.FunctionDef)]
    consts = [n for n in wanted if isinstance(n, ast.Assign)]
    module = ast.fix_missing_locations(ast.Module(body=[*funcs, *consts], type_ignores=[]))
    ns: dict = {}
    exec(compile(module, str(SRC), "exec"), ns)
    return ns


ns = load_constants()


def test_every_serious_injury_label_seen_in_cer_maps_to_one():
    pattern = re.compile(ns["SERIOUS_INJURY_PATTERN"])
    for seen in ["Serious Injury (CER or TSB)", "Serious Injury (CER)", "Serious Injury (as defined in the OPR)"]:
        assert pattern.match(seen)
    assert not pattern.match("Serious Injury")  # already normalized: left alone
    assert "Serious Injury" in ns["CER_INCIDENT_TYPES"]


def test_cause_list_excludes_the_pending_marker():
    # 17 labels were profiled; "To be determined" means no cause yet, not a category.
    assert len(ns["CER_CAUSES"]) == 16
    assert ns["CAUSE_PENDING"] not in ns["CER_CAUSES"]
    assert len(set(ns["CER_CAUSES"])) == len(ns["CER_CAUSES"])


def test_status_values_are_the_three_profiled():
    assert ns["CER_STATUSES"] == ["Initially Submitted", "Submitted", "Closed"]


def test_only_known_builds_a_null_safe_array_check():
    sql = ns["only_known"]("incident_types", ["Fire", "Explosion"])
    assert sql == "size(array_except(coalesce(incident_types, array()), array('Fire', 'Explosion'))) = 0"


def test_only_known_escapes_quotes():
    assert "'Operator\\'s error'" in ns["only_known"]("c", ["Operator's error"])


def test_every_expectation_names_an_output_column():
    output = {
        "incident_number", "reported_date", "status", "incident_types", "immediate_causes", "basic_causes",
        "latitude", "longitude", "volume_reported", "volume_released_m3",
    }
    for name, sql in ns["CER_EXPECTATIONS"].items():
        assert any(col in sql for col in output), name


def test_history_version_is_the_snapshot_date_as_yyyymmdd():
    from datetime import date

    tree = ast.parse((SRC.parent / "history.py").read_text())
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "version_of")
    hist: dict = {"date": date}  # the annotation is evaluated at definition time
    exec(compile(ast.fix_missing_locations(ast.Module(body=[fn], type_ignores=[])), "history.py", "exec"), hist)
    assert hist["version_of"](date(2024, 9, 4)) == 20240904
    assert hist["version_of"](date(2022, 9, 14)) < hist["version_of"](date(2024, 9, 4))


PHMSA_MAPS = ["PHMSA_TEXT", "PHMSA_INT", "PHMSA_DECIMAL", "PHMSA_FLAG", "PHMSA_TIMESTAMP"]


def phmsa_sources():
    for m in PHMSA_MAPS:
        for src in ns[m].values():
            yield src[0] if isinstance(src, tuple) else src


def test_no_personal_contact_column_reaches_phmsa_silver():
    contact = ("preparer_", "local_contact_", "authorizer_", "operator_street", "operator_postal", "operator_city")
    leaked = [s for s in phmsa_sources() if s.startswith(contact) or s == "prepared_date"]
    assert leaked == []


def test_phmsa_silver_names_are_unique():
    names = [n for m in PHMSA_MAPS for n in ns[m]] + ["report_received_date", "narrative", "_snapshot_date"]
    assert len(names) == len(set(names))


def redact(text):
    text = re.sub(ns["EMAIL_PATTERN"], "[EMAIL]", text)
    return re.sub(ns["PHONE_PATTERN"], "[PHONE]", text)


def test_redaction_catches_contact_shapes_seen_in_narratives():
    # Shapes from the 2026-10-30 snapshot, with made-up digits and address.
    assert redact("REPORTS SPILL TO TXRRC (512) 463-6788 AND") == "REPORTS SPILL TO TXRRC [PHONE] AND"
    assert redact("LA STATE POLICE 225-925-6595 REPORT") == "LA STATE POLICE [PHONE] REPORT"
    assert redact("CALLED NRC (800-424-8802) AND") == "CALLED NRC ([PHONE]) AND"
    assert redact("OFFICE 713.369.9000 FAX") == "OFFICE [PHONE] FAX"
    assert redact("contact j.doe@example.com today") == "contact [EMAIL] today"


def test_redaction_leaves_report_facts_alone():
    for text in ["NRC1234567 AT 7/16/2011 19:40", "RELEASED 1,234.56 BBLS", "REPORT # 12-34567", "MP 123.4 TO 456.7"]:
        assert redact(text) == text


TAXONOMY = SRC.parent.parent.parent / "reference" / "cause_taxonomy.csv"


def load_function(path, name, namespace):
    tree = ast.parse(path.read_text())
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    exec(compile(ast.fix_missing_locations(ast.Module(body=[fn], type_ignores=[])), str(path), "exec"), namespace)
    return namespace[name]


taxonomy = load_function(SRC, "read_reference", {"csv": __import__("csv")})(str(TAXONOMY))
phmsa_rows = [r for r in taxonomy if r["source"] == "phmsa"]


def test_taxonomy_csv_has_the_columns_the_pipeline_reads():
    assert list(taxonomy[0]) == ns["TAXONOMY_COLUMNS"]
    assert {r["mapping"] for r in taxonomy} <= set(ns["TAXONOMY_MAPPINGS"])


def test_taxonomy_targets_are_cer_immediate_causes():
    cer = {r["source_cause"] for r in taxonomy if r["source"] == "cer"}
    assert len(cer) == 7 and cer <= set(ns["CER_CAUSES"])
    assert {r["taxonomy_cause"] for r in taxonomy if r["taxonomy_cause"]} == cer


def test_only_excluded_rows_have_no_target():
    for r in taxonomy:
        assert (r["mapping"] == "excluded") == (r["taxonomy_cause"] is None), r["source_cause_detail"]


def test_phmsa_keys_are_unique_and_known():
    keys = [(r["source_cause"], r["source_cause_detail"]) for r in phmsa_rows]
    assert len(keys) == len(set(keys)) == 41
    assert {r["source_cause"] for r in phmsa_rows} == set(ns["PHMSA_CAUSES"])


def test_phmsa_coverage_matches_the_reviewed_counts():
    by_mapping: dict = {}
    for r in phmsa_rows:
        by_mapping[r["mapping"]] = by_mapping.get(r["mapping"], 0) + int(r["incidents"])
    assert by_mapping == {"direct": 5669, "judgment": 333, "excluded": 50}


def test_judgment_calls_carry_a_rationale():
    assert all(r["rationale"] for r in phmsa_rows if r["mapping"] == "judgment")


def test_history_does_not_diff_the_mapped_cause():
    added = set(re.findall(r'(?:alias|withColumn)\("(taxonomy_[a-z]+)"', SRC.read_text()))
    assert added == {"taxonomy_causes", "taxonomy_cause", "taxonomy_mapping"}
    not_diffed = ast.literal_eval(re.search(r"NOT_DIFFED = (\[.*\])", (SRC.parent / "history.py").read_text()).group(1))
    assert added <= set(not_diffed)
