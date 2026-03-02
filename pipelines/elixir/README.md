

# 📘 TOPdesk Ingestion Pipeline

## Overview

This pipeline ingests data from TOPdesk APIs into Databricks **Bronze (Delta)** tables.

It supports:

* multiple endpoints
* incremental loading per endpoint
* batching for performance
* per-endpoint state tracking

---

# ⚙️ Configuration

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

---

# 🔁 How it works

For each endpoint:

1. Read `last_run` from state table
2. Build incremental query (if applicable)
3. Fetch data from API
4. Batch records (≈5000 rows)
5. Write to Bronze table
6. Save latest timestamp back to state table

---

# 🗃️ Data Output

Each endpoint writes to its own Bronze table:

| Endpoint           | Bronze table                                |
| ------------------ | ------------------------------------------- |
| incidents          | `elixir.brz.topdesk_incidents_raw`          |
| time_registrations | `elixir.brz.topdesk_time_registrations_raw` |
| impacts            | `elixir.brz.topdesk_impacts_raw`            |
| branches           | `elixir.brz.topdesk_branches_raw`           |

Schema:

```
raw_json STRING
ingestion_time TIMESTAMP
```

Bronze is **append-only** and may contain duplicates.

---
# 🧠 Incremental & Static Load Strategy

## 🔄 Incremental Logic

* First run → full load
* Next runs → `<field> > last_run` filter
* Each endpoint tracked independently

---

## 📦 Static / Reference Data

Some endpoints represent **reference data** that:

* do not contain `modificationDate` or `creationDate`
* change infrequently
* may vary in size depending on configuration (from small lookup tables to larger lists such as categories or operators)

Examples:

* IncidentPriorities
* IncidentProcessingStatuses
* Categories / Subcategories
* ClosureCodes
* Branches
* BudgetHolders
* OperatorGroups
* ChangeImpacts / ChangePriorities / ChangeProcessingStatuses

---

## 🚫 How static endpoints are handled

* Static endpoints are **not executed on every run**
* They are typically **commented out or disabled in the pipeline**
* They are only executed **manually when needed**

---

## 🔁 When to refresh static data

Refresh these endpoints when:

* configuration changes in TOPdesk
* new categories, priorities, or statuses are created
* organisational structure changes (operators, branches, groups)

Typical refresh frequency:

| Data                       | Suggested refresh |
| -------------------------- | ----------------- |
| Categories / Subcategories | monthly           |
| Priorities / Statuses      | rarely            |
| Operators                  | weekly or daily   |

---

## 📊 Summary

| Data type     | Examples                               | Load strategy                    |
| ------------- | -------------------------------------- | -------------------------------- |
| Transactional | Incidents, TimeRegistrations, Changes  | Incremental (every run)          |
| Reference     | Priorities, Statuses, Categories, etc. | Load once + refresh occasionally |




State table:

```
pipeline | endpoint | last_run
```

---

# ⚡ Performance

Batch writes are used to improve speed and reduce cost:

```
BATCH_SIZE = 5000
```

This avoids:

* too many Spark jobs
* small file problems
* RPC overload

---

# ▶️ Running

Run the pipeline:

```python
main()
```

Modes:

```python
LOAD_TYPE = "FULL"
LOAD_TYPE = "INCREMENTAL"
```

---

# 🚨 Notes

* Bronze layer **may contain duplicates** (by design)
* Deduplication happens in **Silver layer**
* If a run fails, rerun is safe (append-only)

---

# 👤 For new developers

Key things to know:

* Everything is driven by the `ENDPOINTS` config
* Each endpoint has its own state and Bronze table
* Incremental logic uses the `incremental_field`
* Batch size controls performance

---

# ✅ Summary

This pipeline is:

✔ config-driven
✔ incremental per endpoint
✔ cost-efficient
✔ scalable

