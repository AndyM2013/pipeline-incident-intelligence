-- Run once in the SQL editor of your Free Edition workspace.
-- If CREATE CATALOG is refused, use the workspace's default catalog instead:
-- replace pipeline_intel below with its name, and set the `catalog` variable in databricks.yml to match.

CREATE CATALOG IF NOT EXISTS pipeline_intel
  COMMENT 'Portfolio: CER + PHMSA pipeline incident lakehouse';

CREATE SCHEMA IF NOT EXISTS pipeline_intel.bronze COMMENT 'Raw rows per source snapshot, all columns as text';
CREATE SCHEMA IF NOT EXISTS pipeline_intel.silver COMMENT 'Conformed, SCD2, cause taxonomy, operators';
CREATE SCHEMA IF NOT EXISTS pipeline_intel.gold   COMMENT 'Incident facts, LLM extractions, narrative chunks';
CREATE SCHEMA IF NOT EXISTS pipeline_intel.ops    COMMENT 'Evaluation runs, freshness, reconciliation';

-- Landing zone for downloaded files: /Volumes/pipeline_intel/bronze/landing/<source>/<YYYY-MM-DD>/<file>
CREATE VOLUME IF NOT EXISTS pipeline_intel.bronze.landing
  COMMENT 'Dated source snapshots uploaded by scripts/fetch_snapshots.py';

-- Quick check after the first upload (adjust the date folder):
-- LIST '/Volumes/pipeline_intel/bronze/landing/cer/';
