
TransformationManager
    review
        process_transaction
        _generate_signals
        _create_domain_events
        _reconcile_transfers
        _has_decoded_logs
        _get_decoded_logs
        _create_transformer_error
    

TransformationOperations
    review
        create_context
        create_events_from_signals
        create_fallback_events
        _create_trade_events_from_routes
        _create_trade_events_from_swaps
        _create_liquidity_events
        _create_transfer_events
        _create_incomplete_trade_events
        _create_unknown_transfer_events
        _get_known_addresses
        _classify_address
        _determine_trade_direction
        _get_base_token
        _get_quote_token
        _get_base_amount
        _get_quote_amount
        _create_trade_from_single_swap
        _create_trade_from_multi_swap
    good
        _get_tokens_of_interest


TransformerContext
    review
        add_signals
        get_signals_by_type
        mark_signal_consumed
        is_signal_consumed
        mark_transfer_explained
        get_unexplained_transfers
        reconcile_event_transfers
        group_logs_by_contract
        get_transfer_balance_for_token
        get_signals_involving_address
        get_reconciliation_summary
        _get_tokens_of_interest
        _signal_involves_address