def _as_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def main():
    func_input = store.fetch_input()
    label = str(func_input.get("claim_label", "tx"))
    guard_key = str(func_input.get("guard_key", "rc_guard"))
    audit_key = str(func_input.get("audit_key", "rc_audit"))
    hot_key = str(func_input.get("hot_key", "rc_hot"))
    tail_key = str(func_input.get("tail_key", "rc_tail"))
    result_key = str(func_input.get("result_key", "rc_result"))
    expected_guard = str(func_input.get("guard_value", "0"))
    threshold_raw = str(func_input.get("guard_abort_threshold", "")).strip()

    guard_value = str(store.get(guard_key))
    if guard_value != expected_guard:
        store.abort_tx(
            f"guard RYW violation: label={label}, key={guard_key}, "
            f"expected={expected_guard}, got={guard_value}"
        )

    if threshold_raw:
        threshold = _as_int(threshold_raw, 0)
        if _as_int(guard_value, 0) >= threshold:
            store.abort_tx(
                f"repair correctness guard abort: label={label}, key={guard_key}, "
                f"guard={guard_value}, threshold={threshold}"
            )

    audit_value = f"{label}:guard:{guard_value}"
    store.put(audit_key, audit_value)
    store.ret({
        "guard_label": label,
        "guard_value": guard_value,
        "audit_value": audit_value,
        "hot_key": hot_key,
        "tail_key": tail_key,
        "guard_key": guard_key,
        "audit_key": audit_key,
        "result_key": result_key,
    })
