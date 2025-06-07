## Signals Refactor

## Design
logs:
transfers:
signals:
events:
errors:


### Transaction Manager groups PoolSwaps into Trades
### Reconciliation, Event net = BLUB Transfer Net, not just transfers
### Handling for Unknown BLUB events

##
* Pool events are FACTS. Trade events are INTERPRETATIONS.


* Transformers returns (signals, events, transfers, errors)

* transformer.process_signals -

## Notes
* Router/Aggregator signals are mainly for showing intent, determining whether the user or the aggregator is the taker
* Only interested in relevant components of the multi-hop swaps.
* Want to combine all trades of the same direction. One trade for each direction.
* Equal and opposite trades means it was just arbitrage. if both, then if they are within say 1 token then its arbitrage
* Can drop all decoded transfers once everything has been grouped to target token
* Reconciliation is just for the target token

## Questions
1) Do I even need Unmatched Transfers

## Todo
1) Add missing BLUB pool to configs (the launch pool)
2) 




# TRANSFORMERS REVIEW
1) TransformerRegistry
    _load_transformer_classes
    _setup_transformers
    register_contract
    get_transformer
    get_all_contracts
    get_contracts_with_transformers

2) TransformationManager
    process_transaction
    _process_transformers
    _process_transaction_level_events
    _get_decoded_logs
    _has_decoded_logs
    get_processing_summary

3) BaseTransformer
    process_transfers
    process_logs
    _validate_attr
    _create_log_exception
    _create_tx_exception
    _get_unmatched_transfers
    _get_all_transfers
    _get_transfers_for_token
    _get_decoded_logs
    _has_decoded_logs
    _build_transfer_from_log
    _convert_to_matched_transfer
    _create_matched_transfers_dict
    _get_swap_direction
    _get_base_quote_amounts
    _calculate_net_amounts_by_token
    _validate_addresses
    _validate_amounts
    _validate_transfer_count
    _is_router_mediated
    _extract_provider_from_transfers
    _create_error
    _sum_transfer_amounts
    _filter_transfers_by_direction
    _find_transfers_by_criteria

3) TokenTransformer(BaseTransformer)
    _get_transfer_attributes
    process_transfers
    process_logs

4) WavaxTransformer(TokenTransformer)
    _get_transfer_attributes

5) PoolTransformer(BaseTransformer)
    get_amounts
    get_in_out_amounts
    _get_liquidity_transfers
    _get_swap_transfers
    _validate_bin_consistency
    _unpack_lb_amounts
    _create_positions_from_bins
    _aggregate_swap_logs
    _handle_batch_transfers
    _validate_transfer_counts_flexible

6) LfjPoolTransformer(PoolTransformer)
    _create_fee_collection_event
    _handle_mint
    _handle_burn
    _handle_swap
    process_transfers
    process_logs

7) LbPairTransformer(PoolTransformer)
    _prepare_lb_amounts_and_bins
    _validate_lb_liquidity_transfers
    _validate_and_create_lb_positions
    _calculate_net_amounts
    _handle_mint
    _handle_burn
    _handle_swap
    process_transfers
    process_logs