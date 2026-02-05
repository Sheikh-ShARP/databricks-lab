from utils.validation.field_validation import validate_strings
from utils.validation.business_validation import check_functional_dependencies

def run_validation(df, config, id_cols=None):
    report = {}

    if "field_rules" in config:
        report["field"] = validate_strings(df, config["field_rules"].get("string_rules", {}), id_cols)

    if "business_rules" in config:
        report["business"] = check_functional_dependencies(
            df, config["business_rules"].get("functional_dependencies", {}), id_cols
        )

    return report