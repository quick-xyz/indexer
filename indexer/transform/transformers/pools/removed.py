    def _create_positions_from_bins(self, bins: List[int], amounts: List, bin_amounts: Dict, 
                                   tx: Transaction, log: DecodedLog, negative: bool = False) -> Tuple[Dict, str, str, str]:
        positions = {}
        sum_base, sum_quote, sum_receipt = 0, 0, 0
        multiplier = -1 if negative else 1

        for i, bin_id in enumerate(bins):
            base_amount_int, quote_amount_int = self._unpack_lb_amounts(amounts[i])
            base_amount_int = amount_to_int(base_amount_int)
            quote_amount_int = amount_to_int(quote_amount_int)
            receipt_amount = amount_to_int(bin_amounts[bin_id])

            position = Position(
                timestamp=tx.timestamp,
                tx_hash=tx.tx_hash,
                receipt_token=log.contract,
                receipt_id=bin_id,
                amount_base=amount_to_str(base_amount_int * multiplier),
                amount_quote=amount_to_str(quote_amount_int * multiplier),
                amount_receipt=amount_to_str(receipt_amount * multiplier),
            )
            positions[position.content_id] = position
            
            sum_base += base_amount_int
            sum_quote += quote_amount_int
            sum_receipt += receipt_amount

        return positions, amount_to_str(sum_base), amount_to_str(sum_quote), amount_to_str(sum_receipt)



    def _validate_bin_consistency(self, amounts: List, bins: List, transfer_bins: List, 
                                 tx_hash: EvmHash, log_index: int, error_dict: Dict[ErrorId, ProcessingError]) -> bool:
        if len(amounts) != len(bins):
            error = create_transform_error(
                error_type="invalid_lb_transfer",
                message=f"LB: Expected amounts and bins to have the same length, got {len(amounts)} and {len(bins)}",
                tx_hash=tx_hash,
                log_index=log_index
            )
            error_dict[error.error_id] = error
            return False

        if transfer_bins != bins:
            error = create_transform_error(
                error_type="invalid_lb_transfer",
                message="Transfer bins do not match event bins",
                tx_hash=tx_hash,
                log_index=log_index
            )
            error_dict[error.error_id] = error
            return False

        return True

    def _aggregate_swap_logs(self, swap_logs: List[DecodedLog]) -> Tuple[str, str, Dict]:
        net_base_amount, net_quote_amount = 0, 0
        swap_batch = {}

        for log in swap_logs:
            base_amount_in, quote_amount_in = self._unpack_lb_amounts(log.attributes.get("amountsIn"))
            base_amount_out, quote_amount_out = self._unpack_lb_amounts(log.attributes.get("amountsOut"))

            base_amount_in = amount_to_int(base_amount_in)
            quote_amount_in = amount_to_int(quote_amount_in)
            base_amount_out = amount_to_int(base_amount_out)
            quote_amount_out = amount_to_int(quote_amount_out)

            base_amount = base_amount_in - base_amount_out
            quote_amount = quote_amount_in - quote_amount_out
            bin_id = int(log.attributes.get("id"))
            
            swap_batch[bin_id] = {"base": amount_to_str(base_amount), "quote": amount_to_str(quote_amount)}
            net_base_amount += base_amount
            net_quote_amount += quote_amount

        return amount_to_str(net_base_amount), amount_to_str(net_quote_amount), swap_batch



    def _create_fee_collection_event(self, fee_transfers: List[Transfer], tx: Transaction) -> Optional[TransferLedger]:
        if not fee_transfers:
            return None
            
        fee_matched = msgspec.convert(fee_transfers[0], type=MatchedTransfer)
        return TransferLedger(
            timestamp=tx.timestamp,
            tx_hash=tx.tx_hash,
            token=self.contract_address,
            address=fee_matched.to_address,
            amount=fee_matched.amount,
            action="received",
            transfers={fee_matched.content_id: fee_matched},
            desc="Protocol fees collected",
        )

    def _validate_bin_amounts(self, amounts: List[bytes], bins: List[int], log: DecodedLog, errors: Dict[ErrorId, ProcessingError]) -> bool:
        if len(amounts) != len(bins):
            error = create_transform_error(
                error_type="invalid_lb_swap",
                message=f"LB: Expected amounts and bins to have the same length, got {len(amounts)} and {len(bins)}",
                tx_hash=log.tx_hash,
                log_index=log.index
            )
            errors[error.error_id] = error
            return False

        if not all(isinstance(bin_id, int) for bin_id in bins):
            error = create_transform_error(
                error_type="invalid_lb_swap",
                message="LB: All bin IDs must be integers",
                tx_hash=log.tx_hash,
                log_index=log.index
            )
            errors[error.error_id] = error
            return False

        return True