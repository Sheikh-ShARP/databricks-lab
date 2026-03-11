# 📘 TOPdesk Ingestion Pipeline

## Overview

This pipeline ingests data from TOPdesk APIs into Databricks **Bronze (Delta)** tables, transforms and validates it in a **Silver layer**, and applies a **Data Quality framework** to ensure only trustworthy data reaches downstream consumers.

---

## 🏗️ Architecture

```
TOPdesk API
    ↓
Bronze Layer        - raw JSON, append-only, no transformation
    ↓
Silver Layer        - parsed, cleaned, enriched, deduplicated
    ↓
Data Quality        - validated, quarantined if errors found
    ↓
Silver Table        - written only if DQ passes
```

---

# ⚙️ Bronze Layer

## Configuration

All behavior is defined in `ENDPOINTS`:

```python
ENDPOINTS = [
    {
        "name": "incidents",
        "url": "...",
        "fields": None,
        "incremental_field": "modificationDate",
        "bronze_table": "CATALOG.SCHEMA.topdesk_incidents_raw"
    },
    {
        "name": "time_registrations",
        "url": "...",
        "fields": "...",
        "incremental_field": "creationDate",
        "bronze_table": "CATALOG.SCHEMA.topdesk_time_registrations_raw"
    }
]
```

Global settings:

```python
PIPELINE_NAME = "topdesk_ingestion"
STATE_TABLE   = "CATALOG.meta.pipeline_state"
LOAD_TYPE     = "INCREMENTAL"  # or FULL
BATCH_SIZE    = 5000
```

## How it works

For each endpoint:

1. Read `last_run` from state table
2. Build incremental query (if applicable)
3. Fetch data from API
4. Batch records (≈5000 rows)
5. Write to Bronze table
6. Save latest timestamp back to state table

## Data Output

Each endpoint writes to its own Bronze table:

| Endpoint           | Bronze table                                |
| ------------------ | ------------------------------------------- |
| incidents          | `elixir.brz.topdesk_incidents_raw`          |
| time_registrations | `elixir.brz.topdesk_time_registrations_raw` |
| impacts            | `elixir.brz.topdesk_impacts_raw`            |
| branches           | `elixir.brz.topdesk_branches_raw`           |

Schema:

```
raw_json        STRING
ingestion_time  TIMESTAMP
```

Bronze is **append-only** and may contain duplicates. Deduplication happens in the Silver layer.

## Incremental & Static Load Strategy

### Incremental Logic

- First run → full load
- Next runs → `<field> > last_run` filter
- Each endpoint tracked independently

### Static / Reference Data

Some endpoints represent reference data that does not change frequently — examples include `IncidentPriorities`, `ProcessingStatuses`, `Categories`, `Branches`. These are only executed manually when needed, not on every run.

| Data                       | Suggested refresh |
| -------------------------- | ----------------- |
| Categories / Subcategories | Monthly           |
| Priorities / Statuses      | Rarely            |
| Operators                  | Weekly or daily   |

## Performance

```python
BATCH_SIZE = 5000
```

Batch writes reduce small file problems, Spark job overhead, and RPC load.

---

# 🥈 Silver Layer

## What it does

The silver pipeline reads from the Bronze table and produces a clean, enriched, deduplicated version of the data ready for analytics and the DQ framework.

## Pipeline steps

```python
def main():
    # 1. Parse     — extract and type-cast fields from raw JSON
    # 2. Clean     — standardise nulls, trim whitespace, cast types
    # 3. Enrich    — derive new columns (ticket_status, OC, business, domain)
    # 4. Validate  — structural checks (schema, required fields)
    # 5. Scope     — filter to in-scope records only (e.g. OC not null)
    # 6. Dedupe    — keep latest version per incident_id
    # 7. Final     — select and rename final column set
    # 8. DQ        — business rule validation (see DQ Framework below)
    # 9. Write     — write to silver table only if DQ passes
    # 10. Watermark — update last_processed timestamp
```

## Column renames

The silver layer renames raw TopDesk field names to clean snake_case equivalents:

| TopDesk raw field        | Silver column      |
| ------------------------ | ------------------ |
| `id`                     | `ticket_id`        |
| `creationDate`           | `created_date`     |
| `modificationDate`       | `modification_ts`  |
| `processingStatus.name`  | `status`           |
| `caller.dynamicName`     | `caller_name`      |
| `caller.email`           | `caller_email`     |
| `caller.branch.name`     | `country_program`  |
| `category.name`          | `category`         |
| `subcategory.name`       | `subcategory`      |
| `priority.name`          | `priority`         |
| `urgency.name`           | `urgency`          |

## Derived columns

| Column         | Logic |
| -------------- | ----- |
| `ticket_status`| Mapped from raw `status` → `Open` / `Closed` |
| `OC`           | Derived from `category` → `OCA` / `OCG` |
| `business`     | Derived from `category` → e.g. `UniField` |
| `domain`       | Derived from `category` → e.g. `Supply` |

## Output table

```
{catalog}.{schema}.topdesk_incidents
```

## Incremental logic

The silver pipeline tracks its own watermark independently of the bronze pipeline:
+—
```
catalog.meta.pipeline_state
pipeline              | last_processed
topdesk_silver_incidents | 2026-03-10T09:12:27
```

On each run, only bronze records with `ingestion_time > last_processed` are processed.

---

# ✅ Data Quality Framework

## What it does

The DQ framework runs after `build_final_table()` and before `write_to_silver()`. It evaluates a set of configurable business rules against the final DataFrame and:

- Writes a **validation report** - pass rate per rule per run
- Writes **quarantine records** - full failed records with failure reason
- Returns a result that the pipeline uses to decide whether to write to silver or halt

## Where it lives

```
pipelines/elixir/
├── config/
│   └── rules_topdesk_incidents.py   ← rule definitions
├── engine/
│   └── rule_evaluator.py            ← evaluates rules, produces pass/fail
├── writers/
│   ├── report_writer.py             ← writes summary to report table
│   └── quarantine_writer.py         ← writes failed records to quarantine
└── dq_runner.py                     ← orchestrator — call this from notebook
```

## How to use it

```python
from dq_runner import run_dq
import config.rules_topdesk_incidents as rules_topdesk_incidents

dq_result = run_dq(
    spark            = spark,
    rules_config     = rules_topdesk_incidents,
    pipeline_name    = PIPELINE_NAME,
    report_table     = "catalog.meta.dq_validation_report",
    quarantine_table = "catalog.meta.dq_quarantine",
    input_df         = final_df,
)

if not dq_result["passed"]:
    raise Exception(
        f"[DQ] Validation failed — {dq_result['error_failures']} error(s) "
        f"on run {dq_result['run_id']}. Check quarantine table."
    )
```

## Output tables

| Table | What it contains | Granularity |
| ----- | ---------------- | ----------- |
| `catalog.meta.dq_validation_report` | Pass rate per rule per run | 1 row per rule per run |
| `catalog.meta.dq_quarantine` | Full failed records with failure reason | 1 row per failed record per rule |

Both tables are **append-only** - full history is always preserved.

## Severity levels

| Severity | Quarantined? | Pipeline halts? | Use when |
| -------- | ------------ | --------------- | -------- |
| `error`  | ✅ Yes       | ✅ Yes          | Record is fundamentally broken - missing ID, invalid timestamp |
| `warning`| ❌ No        | ❌ No           | Record is suspicious but usable - unknown status value |

## Rule types

| Type | What it checks | Example |
| ---- | -------------- | ------- |
| `not_null` | Column must never be null | `ticket_id` must always exist |
| `unique` | No duplicate values within a run | `ticket_id` must be unique |
| `accepted_values` | Value must be from a known list | `ticket_status` must be a known status |
| `range` | Numeric value within min/max bounds | `priority` between 1 and 5 |
| `regex` | Value matches a pattern | `ticket_id` matches `^[A-Z0-9]+$` |
| `custom_sql` | Any Spark SQL boolean expression | `modification_ts IS NULL OR modification_ts >= created_date` |

## How to add a new rule

Edit `config/rules_topdesk_incidents.py` - no engine changes needed:

```python
RULES = [
    ...
    {
        "rule_id":     "incidents_category_not_null",
        "column":      "category",
        "rule_type":   "not_null",
        "severity":    "error",
        "description": "Every incident must have a category.",
    },
]
```

## How to add a new valid status

Append to `VALID_STATUSES` in `config/rules_topdesk_incidents.py`:

```python
VALID_STATUSES = [
    "open",
    "in progress",
    "waiting for caller",
    ...
    "new status from topdesk",   # ← just add it here
]
```

## Quarantine resolution workflow

When a record is quarantined and the underlying issue is fixed:

```sql
UPDATE catalog.meta.dq_quarantine
SET    resolved      = True,
       resolved_at   = current_timestamp(),
       resolved_note = 'Fixed null ticket_id in source system'
WHERE  run_id  = '<run_id>'
AND    rule_id = 'incidents_id_not_null'
```

Quarantine rows are **never deleted** — the full audit trail is always preserved.

---

# 🚨 Notes

- Bronze layer **may contain duplicates** - by design, deduplication happens in Silver
- Silver writes are **blocked** if any `error`-severity DQ rule fails
- `warning`-severity failures are **visible in the report** but do not block the pipeline
- All DQ tables are **self-bootstrapping** - created automatically on first run
- Re-running after a failure is **safe** - all layers are append-only or idempotent

---

# 👤 For new developers

Key things to know:

- Bronze is driven by `ENDPOINTS` config - add a new endpoint there to ingest a new API
- Silver is driven by the transformation functions - `parse`, `clean`, `enrich`, `validate`, `deduplicate`
- DQ rules are driven by `config/rules_topdesk_incidents.py` - add rules there, no code changes needed
- All state (watermarks) lives in `catalog.meta.pipeline_state`
- All DQ history lives in `catalog.meta.dq_validation_report` and `catalog.meta.dq_quarantine`