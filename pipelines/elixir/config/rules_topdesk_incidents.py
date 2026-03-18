# config/rules_topdesk_incidents.py
# -----------------------------------------------------------
# Data Quality rule definitions for the topdesk_incidents
# silver table.
#
# HOW TO ADD A NEW RULE:
#   1. Add a new dict to the RULES list below.
#   2. Set rule_type to one of:
#      not_null | unique | accepted_values | range | regex | custom_sql
#   3. Set severity to "error" (quarantined) or "warning" (flagged only).
#   4. That's it - the engine picks it up automatically.
#
# HOW TO ADD A NEW VALID STATUS:
#   Just append to VALID_STATUSES below. No engine changes needed.
# -----------------------------------------------------------

SOURCE_TABLE = "{catalog}.{schema}.topdesk_incidents"  # <-- replace with your catalog/schema

# Extend this list as TopDesk introduces new processing statuses.
VALID_STATUSES = [
    "In Progress",
    "Waiting for Caller",
    "Waiting for Supplier",
    "Waiting for Approval",
    "Waiting for Release",
    "New",
    "Assigned",
    "Completed",
    "Cancelled",
    "Closed",
    "Reopened",
    "Response received"
]
# ------------------------------------------------------------------
# Define a list of columns that must never be null.
NOT_NULL_COLUMNS = ["ticket_id", "status", "ticket_status", "created_date", "category"]

# Auto-generate not_null rules for all columns
NOT_NULL_RULES = [
    {
    "rule_id":     f"incidents_{col}_not_null",
    "column":      col,
    "rule_type":   "not_null",
    "severity":    "error",
    "description": f"Column '{col}' must never be null.",
    } 
    for col in NOT_NULL_COLUMNS
]

RULES = NOT_NULL_RULES + [
   
    # {
    #     "rule_id":     "incidents_id_not_null",
    #     "column":      "ticket_id",
    #     "rule_type":   "not_null",
    #     "severity":    "error",
    #     "description": "Primary key 'id' must never be null.",
    # },
    
    # ------------------------------------------------------------------
    # id - primary key, must be unique within each pipeline run
    #
    {
        "rule_id":     "incidents_id_unique",
        "column":      "ticket_id",
        "rule_type":   "unique",
        "severity":    "error",
        "description": "Primary key 'id' must be unique within each pipeline run.",
    },

    {
        "rule_id":     "incidents_status_accepted_values",
        "column":      "status",
        "rule_type":   "accepted_values",
        "severity":    "warning",       # warning: new statuses can appear from TopDesk
        "params": {
            "values":  VALID_STATUSES,
            "case_sensitive": False
        },
        "description": "Status must be one of the known TopDesk processing statuses (case-insensitive).",
    },

    # ------------------------------------------------------------------
    # modification_ts - optional, but must be valid when present.
    #
    # An incident that was never modified will have modification_ts = null.
    # That is valid. We flag it as a warning for operational visibility,
    # but we never quarantine a record purely because it was never modified.
    # ------------------------------------------------------------------
    {
        "rule_id":     "incidents_modification_ts_not_before_created_date",
        "column":      "modification_ts",
        "rule_type":   "custom_sql",
        "severity":    "error",
        "params": {
            "expression": "modification_ts IS NULL OR modification_ts >= created_date",
        },
        "description": "When present, modification_ts must not precede created_date.",
    },
    {
        "rule_id":     "incidents_modification_ts_null_flag",
        "column":      "modification_ts",
        "rule_type":   "not_null",
        "severity":    "warning",
        "description": "Flags incidents that have never been modified. Not an error - operational visibility only.",
    },
]