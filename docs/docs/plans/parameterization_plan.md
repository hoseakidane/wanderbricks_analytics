# Plan: Full Parameterization of WanderBricks Analytics

## Context

The WanderBricks Analytics DAB project has ~60% of its configuration parameterized (catalog, schema, lakebase settings), but several values remain hard-coded across notebooks and SQL files. This creates fragility — changing table names or lakebase settings requires editing multiple files instead of a single YAML config. This plan eliminates remaining hard-coded references so the pipeline is fully configurable through `databricks.yml` and the resource YAMLs.

**Out of scope:**
- `samples.wanderbricks` source references remain hard-coded — this is stable Databricks sample data and does not vary across environments.
- Markdown documentation cells (`# MAGIC %md`) in `click_stream_data_gen.py` reference `hosea.default.clickstream_synthetic_v3` in ~10 places. These are non-executable documentation from the original development and are left as-is.

---

## New Variables (added to `databricks.yml`)

| Variable | Default | Purpose |
|----------|---------|---------|
| `clickstream_table_name` | `"clickstream_synthetic_v3"` | Generated clickstream table name (shared between data gen and ETL) |
| `lakebase_schema` | `"gold"` | Target schema in Lakebase catalog |
| `lakebase_database_name` | `"databricks_postgres"` | PostgreSQL database name in Lakebase |
| `storage_schema` | `"wanderbricks_lakebase"` | Schema for Lakebase sync pipeline metadata |

All defaults preserve current behavior — no breaking changes on deploy.

---

## Design Notes

**STORAGE_CATALOG coupling:** In `sync_to_lakebase.py`, `STORAGE_CATALOG` (line 78) reuses the main `catalog` widget parameter — sync pipeline metadata is intentionally stored alongside pipeline output. This coupling is by design: the metadata schema lives in the same catalog as the gold tables it tracks. No separate variable is needed unless metadata storage must move to a different catalog.

---

## Implementation Steps

### Step 1: Add variables to `databricks.yml`
- **File:** `databricks.yml`
- Add 4 new variables (with descriptions and defaults) after the existing `lakebase_instance_name` variable block (after line 26)
- No target-specific overrides needed — defaults match both dev and prod

### Step 2: Wire variables through resource YAMLs

**`resources/wanderbricks_pipeline.yml`** (line 15-17):
- Add `clickstream_table_name` to the `configuration:` block
- This becomes available as `${clickstream_table_name}` in the DLT SQL

**`resources/wanderbricks_workflow.yml`**:
- Task `generate-clickstream-data` (line 10-12): Add `clickstream_table_name` to `base_parameters`
- Task `sync-to-lakebase` (line 26-31): Add `lakebase_schema`, `lakebase_database_name`, `storage_schema` to `base_parameters`

### Step 3: Update `sync_to_lakebase.py`
- **File:** `src/notebooks/sync_to_lakebase.py`
- Add 3 widget declarations after line 64: `lakebase_schema`, `lakebase_database_name`, `storage_schema`
- Replace 3 hard-coded assignments:
  - Line 72: `LAKEBASE_SCHEMA = "gold"` → `dbutils.widgets.get("lakebase_schema")`
  - Line 74: `LAKEBASE_DATABASE_NAME = "databricks_postgres"` → `dbutils.widgets.get("lakebase_database_name")`
  - Line 79: `STORAGE_SCHEMA = "wanderbricks_lakebase"` → `dbutils.widgets.get("storage_schema")`

### Step 4: Update `wanderbricks_etl.sql`
- **File:** `src/pipelines/wanderbricks_etl.sql`
- Replace hard-coded `clickstream_synthetic_v3` with `${clickstream_table_name}` on line 37:
  - `FROM STREAM(${source_catalog}.${source_schema}.clickstream_synthetic_v3)` → `FROM STREAM(${source_catalog}.${source_schema}.${clickstream_table_name})`

### Step 5: Update `click_stream_data_gen.py`
- **File:** `src/notebooks/click_stream_data_gen.py`
- Add 1 widget declaration after existing widgets (line 385): `clickstream_table_name` with default `"clickstream_synthetic_v3"`
- Add variable assignment: `clickstream_table_name = dbutils.widgets.get("clickstream_table_name")`
- Update `target_table` construction (line 388):
  - `target_table = f"{catalog}.{schema}.clickstream_synthetic_v3"` → `target_table = f"{catalog}.{schema}.{clickstream_table_name}"`
- **Fix variable shadowing (line 838):** Rename `schema = StructType([...])` to `non_booking_schema = StructType([...])` and update the reference on line 849. The PySpark StructType currently overwrites the `schema` widget variable — this works today because `target_table` is already built by line 388, but would cause a subtle bug if any later code references `schema` expecting the catalog schema name.

---

## Verification

1. **Static:** `databricks bundle validate` and `databricks bundle validate -t prod` — confirms YAML syntax and variable resolution
2. **Deploy:** `databricks bundle deploy` to dev — inspect workflow in UI to confirm all parameters appear with correct defaults
3. **Functional:** `databricks bundle run wanderbricks-workflow` — all 3 tasks should succeed with identical behavior to pre-change
