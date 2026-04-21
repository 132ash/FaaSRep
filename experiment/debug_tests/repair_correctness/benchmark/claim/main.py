def _as_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def main():
    func_input = store.fetch_input()
    label = str(func_input.get("tx_label", "tx"))
    delta = _as_int(func_input.get("delta", "1"), 1)
    guard_delta = _as_int(func_input.get("guard_delta", "0"), 0)
    hot_key = str(func_input.get("hot_key", "rc_hot"))
    tail_key = str(func_input.get("tail_key", "rc_tail"))
    guard_key = str(func_input.get("guard_key", "rc_guard"))
    audit_key = str(func_input.get("audit_key", "rc_audit"))
    result_key = str(func_input.get("result_key", "rc_result"))
    guard_abort_threshold = str(func_input.get("guard_abort_threshold", "")).strip()

    current_value = _as_int(store.get(hot_key), 0)
    next_value = current_value + delta
    store.put(hot_key, str(next_value))

    guard_seen = _as_int(store.get(guard_key), 0)
    guard_value = guard_seen + guard_delta
    store.put(guard_key, str(guard_value))

    store.ret({
        "claim_seen": str(current_value),
        "claim_value": str(next_value),
        "claim_label": label,
        "guard_seen": str(guard_seen),
        "guard_value": str(guard_value),
        "guard_abort_threshold": guard_abort_threshold,
        "hot_key": hot_key,
        "tail_key": tail_key,
        "guard_key": guard_key,
        "audit_key": audit_key,
        "result_key": result_key,
    })
