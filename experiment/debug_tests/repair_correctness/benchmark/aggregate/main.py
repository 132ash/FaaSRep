def main():
    func_input = store.fetch_input()
    label = str(func_input.get("ryw_label", "tx"))
    expected_value = str(func_input.get("ryw_value", "0"))
    guard_value = str(func_input.get("guard_value", "0"))
    expected_audit = str(func_input.get("audit_value", ""))
    hot_key = str(func_input.get("hot_key", "rc_hot"))
    tail_key = str(func_input.get("tail_key", "rc_tail"))
    guard_key = str(func_input.get("guard_key", "rc_guard"))
    audit_key = str(func_input.get("audit_key", "rc_audit"))
    result_key = str(func_input.get("result_key", "rc_result"))

    tail_value = str(store.get(tail_key))
    if not tail_value.endswith(f":{expected_value}"):
        store.abort_tx(
            f"tail propagation violation: label={label}, key={tail_key}, "
            f"expected_value={expected_value}, tail={tail_value}"
        )

    audit_value = str(store.get(audit_key))
    if audit_value != expected_audit:
        store.abort_tx(
            f"audit propagation violation: label={label}, key={audit_key}, "
            f"expected={expected_audit}, got={audit_value}"
        )

    result_value = f"{label}:hot:{expected_value}:guard:{guard_value}"
    store.put(result_key, result_value)
    store.ret({
        "final_value": expected_value,
        "final_label": label,
        "final_guard_value": guard_value,
        "final_audit_value": audit_value,
        "final_result_value": result_value,
        "final_hot_key": hot_key,
        "final_tail_key": tail_key,
        "final_guard_key": guard_key,
        "final_audit_key": audit_key,
        "final_result_key": result_key,
    })
