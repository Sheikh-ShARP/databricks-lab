from pyspark.sql.functions import col, lit, trim, upper, instr, length
from functools import reduce

VALIDATOR_NAME = "field_string_validation"
VALIDATOR_VERSION = "1.0.0"


def validate_strings(df, string_rules: dict, id_cols=None):
    id_cols = id_cols or []
    violations = []

    for column, column_cfg in string_rules.items():

        if column not in df.columns:
            continue

        rule_list = column_cfg.get("rules", [])

        base_select = [col(c) for c in id_cols] + [
            lit(column).alias("column"),
            col(column).alias("value"),
        ]

        for rule in rule_list:
            rule_type = rule.get("type")

            # REQUIRED
            if rule_type == "required":
                rule_df = df.filter(col(column).isNull())

                violations.append(
                    rule_df.select(*base_select, lit("NULL_NOT_ALLOWED").alias("rule"))
                )

            # REGEX
            elif rule_type == "regex":
                pattern = rule.get("pattern")
                rule_df = df.filter(col(column).isNotNull() & ~col(column).rlike(pattern))

                violations.append(
                    rule_df.select(*base_select, lit("PATTERN_MISMATCH").alias("rule"))
                )

            # NO WHITESPACE
            elif rule_type == "no_spaces":
                rule_df = df.filter(col(column).isNotNull() & ~col(column).rlike(r"^\S+$"))

                violations.append(
                    rule_df.select(*base_select, lit("CONTAINS_WHITESPACE").alias("rule"))
                )

            # ALLOWED VALUES
            elif rule_type == "allowed_values":
                allowed = [v.upper() for v in rule.get("values", [])]

                normalized_col = upper(trim(col(column)))

                rule_df = df.filter(
                    col(column).isNotNull() & ~normalized_col.isin(allowed)
                )

                violations.append(
                    rule_df.select(*base_select, lit("INVALID_VALUE").alias("rule"))
                )

    rows_checked = df.count()

    if not violations:
        return {
            "status": "PASS",
            "issues": None,
            "metrics": {
                "rows_checked": rows_checked,
                "violations_count": 0
            },
            "validator": VALIDATOR_NAME,
            "version": VALIDATOR_VERSION
        }

    all_violations = reduce(lambda a, b: a.unionByName(b), violations)
    violation_count = all_violations.count()
    
    if violation_count == 0:
        return {
            "status": "PASS",
            "issues": None,
            "metrics": {
                "rows_checked": rows_checked,
                "violations_count": 0
            },
            "validator": VALIDATOR_NAME,
            "version": VALIDATOR_VERSION
        }

    return {
        "status": "FAIL",
        "issues": all_violations,
        "metrics": {
            "rows_checked": rows_checked,
            "violations_count": violation_count
        },
        "validator": VALIDATOR_NAME,
        "version": VALIDATOR_VERSION
    }
