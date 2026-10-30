"""Bronze: one row per source row per snapshot, every column kept as text.

Reads dated snapshot folders from the landing Volume with Auto Loader:
    <landing_root>/<source>/<YYYY-MM-DD>/<file>

Design choices (worth saying out loud in an interview):
- No type inference. Regulators change formats; Bronze must never fail or silently coerce.
  Types are applied in Silver, where a failed cast is a visible data-quality result.
- Column names are snake_cased so Delta accepts them (CER headers contain spaces and punctuation).
- A column the regulator adds later becomes a real column (addNewColumns: the update stops once
  and the pipeline restarts with the wider schema). Rows that still fail to parse land in
  _rescued_data, and an expectation counts them.
- _snapshot_date comes from the folder name, so Silver can rebuild history snapshot by snapshot.
"""

import re

from pyspark.sql import functions as F

try:  # current Lakeflow Spark Declarative Pipelines API
    from pyspark import pipelines as dp
except ImportError:  # older runtimes
    import dlt as dp  # type: ignore[no-redef]

LANDING_ROOT = spark.conf.get("landing_root")  # noqa: F821  (spark is provided by the pipeline runtime)

# Per-source reader settings. Confirm encodings and quoting against the real files in Week 0.
SOURCES = {
    "cer": {
        "table": "cer_incident_raw",
        "comment": "CER incidents at CER-regulated pipelines and facilities; raw text columns per snapshot.",
        "glob": "*.csv",
        "options": {
            "header": "true",
            "multiLine": "true",  # description fields can contain line breaks inside quotes
            "escape": '"',
            "encoding": "windows-1252",  # checked 2026-10-30: UTF-8 garbles names like "Ile des Chênes"
        },
    },
    "phmsa_liquid": {
        "table": "phmsa_liquid_raw",
        "comment": "PHMSA hazardous liquid accidents, Jan 2010 to present; raw text columns per snapshot.",
        "glob": "*.txt",
        "options": {
            "header": "true",
            "sep": "\t",
            "encoding": "windows-1252",
            # If narratives contain stray double quotes that break rows, disable quoting: "quote": "\u0000"
        },
    },
}


def snake_case(name: str) -> str:
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()
    if not cleaned:
        cleaned = "col"
    return f"c_{cleaned}" if cleaned[0].isdigit() else cleaned


def unique_names(names: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    out = []
    for n in names:
        base = snake_case(n)
        seen[base] = seen.get(base, 0) + 1
        out.append(base if seen[base] == 1 else f"{base}_{seen[base]}")
    return out


def raw_stream(source: str, cfg: dict):
    df = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("pathGlobFilter", cfg["glob"])
        .options(**cfg["options"])
        .load(f"{LANDING_ROOT}/{source}/")
    )
    names = unique_names(df.columns)
    return df.select(
        *[F.col(f"`{old}`").alias(new) for old, new in zip(df.columns, names)],
        F.to_date(F.regexp_extract(F.col("_metadata.file_path"), r"/(\d{4}-\d{2}-\d{2})/", 1)).alias("_snapshot_date"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )


def define_bronze_table(source: str, cfg: dict) -> None:
    @dp.table(name=cfg["table"], comment=cfg["comment"])
    @dp.expect("snapshot_date_parsed", "_snapshot_date IS NOT NULL")
    @dp.expect("no_rescued_data", "_rescued_data IS NULL")
    def _bronze():
        return raw_stream(source, cfg)


for _source, _cfg in SOURCES.items():
    define_bronze_table(_source, _cfg)
