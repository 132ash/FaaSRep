def main():
    func_input = store.fetch_input()
    label = str(func_input.get("claim_label", "tx"))
    expected_value = str(func_input.get("claim_value", "0"))
    hot_key = str(func_input.get("hot_key", "rc_hot"))
    tail_key = str(func_input.get("tail_key", "rc_tail"))
    guard_key = str(func_input.get("guard_key", "rc_guard"))
    audit_key = str(func_input.get("audit_key", "rc_audit"))
    result_key = str(func_input.get("result_key", "rc_result"))
    ryw_value = str(store.get(hot_key))

    if ryw_value != expected_value:
        store.abort_tx(
            f"RYW violation: label={label}, key={hot_key}, expected={expected_value}, got={ryw_value}"
        )

    store.put(tail_key, f"{label}:{ryw_value}")
    store.ret({
        "ryw_value": ryw_value,
        "ryw_label": label,
        "hot_key": hot_key,
        "tail_key": tail_key,
        "guard_key": guard_key,
        "audit_key": audit_key,
        "result_key": result_key,
    })
