from pyspark.sql.functions import col, lit, trim, upper, instr
from functools import reduce

def validate_strings(df, rules, id_cols=None):
    id_cols = id_cols or []
    violations = []

    for column, config in rules.items():

        if column not in df.columns:
            continue

        rule_list = config.get("rules", [])

        base_select = [col(c) for c in id_cols] + [
            lit(column).alias("column"),
            col(column).alias("value"),
        ]

        for rule in rule_list:
            rule_type = rule.get("type")

            # ---- REQUIRED ----
            if rule_type == "required":
                violations.append(
                    df.filter(col(column).isNull())
                      .select(*base_select, lit("NULL_NOT_ALLOWED").alias("rule"))
                )

            # ---- NO SPACES ----
            elif rule_type == "no_spaces":
                violations.append(
                    df.filter(col(column).isNotNull() & (instr(trim(col(column)), " ") > 0))
                      .select(*base_select, lit("CONTAINS_SPACES").alias("rule"))
                )

            # ---- ALLOWED VALUES ----
            elif rule_type == "allowed_values":
                allowed = [v.upper() for v in rule.get("values", [])]

                violations.append(
                    df.filter(col(column).isNotNull() & ~upper(col(column)).isin(allowed))
                      .select(*base_select, lit("INVALID_VALUE").alias("rule"))
                )

    if not violations:
        return None

    return reduce(lambda a, b: a.unionByName(b), violations)