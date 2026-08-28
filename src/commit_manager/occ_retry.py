RETRY_ABORT_METADATA_KEY = 'retry_abort_func'
NO_RETRY_ABORT = 'NONE'


def is_occ_request(transaction_metadata):
    """Return whether the experiment selected this request for OCC handling."""
    return (
        (transaction_metadata or {}).get(
            RETRY_ABORT_METADATA_KEY, NO_RETRY_ABORT
        ) != NO_RETRY_ABORT
    )


def transaction_is_clean(subjection_per_function):
    """A request is OCC-valid only when every observed function is clean."""
    return all(
        not function_state.get('dirty', False)
        for function_state in (subjection_per_function or {}).values()
    )


def remove_transaction_dependencies(transaction_id, pessi_sink_info):
    """Remove dependency edges whose successor is an OCC-retried request."""
    for key in ('batch_sub', 'tx_sub'):
        dependency_table = pessi_sink_info[key]
        for predecessor, successors in list(dependency_table.items()):
            dependency_table[predecessor] = [
                successor for successor in successors
                if successor != transaction_id
            ]
            if not dependency_table[predecessor]:
                dependency_table.pop(predecessor)

    whole_tx_sub = pessi_sink_info['whole_tx_sub']
    for predecessor, successors in list(whole_tx_sub.items()):
        successors.pop(transaction_id, None)
        if not successors:
            whole_tx_sub.pop(predecessor)
    pessi_sink_info['last_tx'].pop(transaction_id, None)
