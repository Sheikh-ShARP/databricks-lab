from pyspark.sql.functions import col, countDistinct, lit

def check_functional_dependencies(df, fd_rules: dict, id_cols=None):
    """
    Checks functional dependencies like sku -> product_type.
    Returns a DataFrame of violations.
    """
    id_cols = id_cols or []
    violations = []

    for determinant, dependent in fd_rules.items():
        if determinant not in df.columns or dependent not in df.columns:
            continue

        v_df = (
            df.groupBy(determinant)
              .agg(countDistinct(dependent).alias("distinct_count"))
              .filter(col("distinct_count") > 1)
              .select(
                  *[col(c) for c in id_cols],
                  lit(determinant).alias("column"),
                  lit(dependent).alias("dependent_column"),
                  lit("FUNCTIONAL_DEPENDENCY_VIOLATION").alias("rule")
              )
        )

        violations.append(v_df)

    if not violations:
        return None

    from functools import reduce
    return reduce(lambda a, b: a.unionByName(b), violations)