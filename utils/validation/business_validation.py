# from pyspark.sql.functions import col, countDistinct, lit

# def check_functional_dependencies(df, fd_rules: dict, id_cols=None):
#     """
#     Checks functional dependencies like sku -> product_type.
#     Returns a DataFrame of violations.
#     """
#     id_cols = id_cols or []
#     violations = []

#     for determinant, dependent in fd_rules.items():
#         if determinant not in df.columns or dependent not in df.columns:
#             continue

#         v_df = (
#             df.groupBy(determinant)
#               .agg(countDistinct(dependent).alias("distinct_count"))
#               .filter(col("distinct_count") > 1)
#               .select(
#                   *[col(c) for c in id_cols],
#                   lit(determinant).alias("column"),
#                   lit(dependent).alias("dependent_column"),
#                   lit("FUNCTIONAL_DEPENDENCY_VIOLATION").alias("rule")
#               )
#         )

#         violations.append(v_df)

#     if not violations:
#         return None

#     from functools import reduce
#     return reduce(lambda a, b: a.unionByName(b), violations)

from pyspark.sql import functions as F
from functools import reduce

VALIDATOR_NAME = "functional_dependency_validation"
VALIDATOR_VERSION = "1.0.0"


def check_functional_dependencies(df, fd_rules: dict, id_cols=None):
    """
    Validate functional dependencies like:
        {"sku": "product_type"}
        {("country", "sku"): "currency"}
    """

    id_cols = id_cols or []
    violations = []

    for determinant, dependent in fd_rules.items():

        det_cols = [determinant] if isinstance(determinant, str) else list(determinant)

        # Skip invalid configs
        if not set(det_cols + [dependent]).issubset(df.columns):
            continue

        grouped = (
            df.select(*(det_cols + [dependent] + id_cols))
              .groupBy(*det_cols)
              .agg(
                  F.countDistinct(dependent).alias("distinct_count"),
                  F.collect_set(dependent).alias("conflicting_values")
              )
              .filter(F.col("distinct_count") > 1)
        )

        v_df = (
            grouped
            .withColumn("column", F.lit(", ".join(det_cols)))
            .withColumn("dependent_column", F.lit(dependent))
            .withColumn("rule", F.lit("FUNCTIONAL_DEPENDENCY_VIOLATION"))
            .select(
                *[F.col(c) for c in det_cols],
                *[F.col(c) for c in id_cols],
                "column",
                "dependent_column",
                "conflicting_values",
                "rule"
            )
        )

        violations.append(v_df)

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
