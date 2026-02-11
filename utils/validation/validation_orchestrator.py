from utils.validation.field_validation import validate_strings
from utils.validation.business_validation import check_functional_dependencies
#from utils.validation.reference_validation import check_reference

import time
import traceback

def _safe_run(name, fn, *args, **kwargs):
    start = time.time()
    try:
        result = fn(*args, **kwargs)

        return {
            "status": "SUCCESS",
            "result": result,
            "error": None,
            "duration_ms": int((time.time() - start) * 1000)
        }

    except Exception as e:
        return {
            "status": "SYSTEM_ERROR",
            "result": None,
            "error": {
                "validator": name,
                "type": type(e).__name__,
                "message": str(e),
                "trace": traceback.format_exc(limit=5)
            },
            "duration_ms": int((time.time() - start) * 1000)
        }


def _skipped(reason):
    return {
        "status": "SKIPPED",
        "result": None,
        "error": None,
        "reason": reason,
        "duration_ms": 0
    }


def run_validation(df, config, id_cols=None):
    report = {
        "meta": {
            "validators_run": [],
            "validators_skipped": [],
            "errors_detected": False,
            "system_errors": False
        }
    }

    # FIELD VALIDATION
    if "field_rules" in config:
        field_rules = config["field_rules"].get("string_rules", {})
        report["field"] = _safe_run(
            "field_validation",
            validate_strings,
            df,
            field_rules,
            id_cols
        )
        report["meta"]["validators_run"].append("field_validation")
    else:
        report["field"] = _skipped("No field_rules configured")
        report["meta"]["validators_skipped"].append("field_validation")

    # BUSINESS VALIDATION
    if "business_rules" in config:
        biz_rules = config["business_rules"].get("functional_dependencies", {})
        report["business"] = _safe_run(
            "business_validation",
            check_functional_dependencies,
            df,
            biz_rules,
            id_cols
        )
        report["meta"]["validators_run"].append("business_validation")
    else:
        report["business"] = _skipped("No business_rules configured")
        report["meta"]["validators_skipped"].append("business_validation")

    # ---- FINAL STATUS EVALUATION ----
    for section in ["field", "business"]:
        r = report.get(section)
        if not r:
            continue

        if r["status"] == "SYSTEM_ERROR":
            report["meta"]["system_errors"] = True

        elif r["status"] == "SUCCESS":
            validation_result = r["result"]

            if isinstance(validation_result, dict) and validation_result.get("status") == "FAIL":
                report["meta"]["errors_detected"] = True


    if report["meta"]["system_errors"]:
        report["final_status"] = "VALIDATION_SYSTEM_ERROR"
    elif report["meta"]["errors_detected"]:
        report["final_status"] = "VALIDATION_FAILED"
    elif report["meta"]["validators_run"]:
        report["final_status"] = "VALIDATION_PASSED"
    else:
        report["final_status"] = "VALIDATION_SKIPPED"
    
    return report

