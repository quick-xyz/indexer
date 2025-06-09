# indexer/transform/transformers/pools/lb_pair.py

from typing import List, Dict, Tuple, Optional

from .pool_base import PoolTransformer
from ....types import (
    DecodedLog,
    Transaction,
    EvmAddress,
    DomainEvent,
    ProcessingError,
    Transfer,
    UnmatchedTransfer,
    Liquidity,
    PoolSwap,
    DomainEventId,
    ErrorId,
    create_transform_error,
)
from ....utils.amounts import amount_to_str, amount_to_int, compare_amounts


class LbPairTransformer(PoolTransformer):
    def __init__(self, contract: EvmAddress, token_x: str, token_y: str, base_token: str):
        super().__init__(contract,token_x,token_y,base_token,fee_collector=None)

    @property
    def token_x(self):
        return self.token0
    
    @property 
    def token_y(self):
        return self.token1

    def _prepare_lb_amounts_and_bins(self, log: DecodedLog) -> Tuple[List[bytes], List[int]]:
        amounts = [bytes.fromhex(amt.replace('0x', '')) for amt in log.attributes.get("amounts")]
        bins = [int(bin_id) for bin_id in log.attributes.get("ids")]
        return amounts, bins

    def _validate_lb_liquidity_transfers(self, liq_transfers: Dict, provider: EvmAddress, 
                                       is_mint: bool, tx_hash: str, log_index: int, 
                                       error_dict: Dict) -> Tuple[List, List, List, List]:
        base_transfers = [t for t in liq_transfers["deposits"].values() if t.token == self.base_token]
        quote_transfers = [t for t in liq_transfers["deposits"].values() if t.token == self.quote_token]
        
        if is_mint:
            pool_operations = list(liq_transfers["mints"].values())
            base_refunds = [t for t in liq_transfers["withdrawals"].values() 
                          if t.token == self.base_token and t.to_address == provider]
            quote_refunds = [t for t in liq_transfers["withdrawals"].values() 
                           if t.token == self.quote_token and t.to_address == provider]
            
            transfer_groups = [
                (base_transfers, "base_deposits", 1, True),
                (quote_transfers, "quote_deposits", 1, True),
                (pool_operations, "pool_mints", 1, False),
                (base_refunds, "base_refunds", 1, True),
                (quote_refunds, "quote_refunds", 1, True)
            ]
            
            if not self._validate_transfer_counts_flexible(transfer_groups, tx_hash, log_index, 
                                                         "invalid_liquidity_deposit_transfers", error_dict):
                return None, None, None, None
                
            return base_transfers, quote_transfers, pool_operations, (base_refunds + quote_refunds)
        else:
            pool_operations = list(liq_transfers["burns"].values())
            
            transfer_groups = [
                (base_transfers, "base_withdrawals", 1, True),
                (quote_transfers, "quote_withdrawals", 1, True),
                (pool_operations, "pool_burns", 1, False)
            ]
            
            if not self._validate_transfer_counts_flexible(transfer_groups, tx_hash, log_index, 
                                                         "invalid_liquidity_withdrawal_transfers", error_dict):
                return None, None, None, None
                
            return base_transfers, quote_transfers, pool_operations, []    

    def _validate_and_create_lb_positions(self, amounts: List, bins: List, pool_operations: List,
                                        tx: Transaction, log: DecodedLog, is_mint: bool,
                                        error_dict: Dict) -> Tuple[Optional[Dict], str, str, str]:
        operation_amounts = pool_operations[0].batch
        operation_bins = list(operation_amounts.keys())
        if not self._validate_bin_consistency(amounts, bins, operation_bins, tx.tx_hash, log.index, error_dict):
            return None, "0", "0", "0"

        positions, sum_base, sum_quote, sum_receipt = self._create_positions_from_bins(
            bins, amounts, operation_amounts, tx, log, negative=not is_mint
        )
        
        return positions, sum_base, sum_quote, sum_receipt

    def _calculate_net_amounts(self, base_transfers: List, quote_transfers: List, 
                             refunds: List) -> Tuple[str, str]:
        base_amount = base_transfers[0].amount if base_transfers else "0"
        quote_amount = quote_transfers[0].amount if quote_transfers else "0"
        
        for refund in refunds:
            if refund.token == self.base_token:
                base_amount = amount_to_str(amount_to_int(base_amount) - amount_to_int(refund.amount))
            elif refund.token == self.quote_token:
                quote_amount = amount_to_str(amount_to_int(quote_amount) - amount_to_int(refund.amount))
                
        return base_amount, quote_amount

    def _handle_mint(self, log: DecodedLog, tx: Transaction) -> Dict[str, Dict]:
        result = {"transfers": {}, "events": {}, "errors": {}}
        
        try:
            provider = EvmAddress(str(log.attributes.get("to")).lower())
            amounts, bins = self._prepare_lb_amounts_and_bins(log)

            if not self._validate_attr([provider, amounts, bins], tx.tx_hash, log.index, result["errors"]):
                return result

            unmatched_transfers = self._get_unmatched_transfers(tx)
            liq_transfers = self._get_liquidity_transfers(unmatched_transfers)

            base_deposits, quote_deposits, pool_mints, refunds = self._validate_lb_liquidity_transfers(
                liq_transfers, provider, True, tx.tx_hash, log.index, result["errors"]
            )
            if base_deposits is None:
                return result

            positions, sum_base, sum_quote, sum_receipt = self._validate_and_create_lb_positions(
                amounts, bins, pool_mints, tx, log, True, result["errors"]
            )
            if positions is None:
                return result

            net_base, net_quote = self._calculate_net_amounts(base_deposits, quote_deposits, refunds)

            if not (compare_amounts(sum_base, net_base) == 0 and compare_amounts(sum_quote, net_quote) == 0 and compare_amounts(sum_receipt, pool_mints[0].amount) == 0):
                error = create_transform_error(
                    error_type="invalid_liquidity_deposit_transfers",
                    message="Sum of amounts in positions does not match net deposit transfer amounts",
                    tx_hash=tx.tx_hash, log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result

            transfers_to_match = base_deposits + quote_deposits + pool_mints + refunds
            matched_transfers = self._handle_batch_transfers(transfers_to_match)

            liquidity = Liquidity(
                timestamp=tx.timestamp,
                tx_hash=tx.tx_hash,
                pool=log.contract,
                provider=provider,
                base_token=self.base_token,
                amount_base=net_base,
                quote_token=self.quote_token,
                amount_quote=net_quote,
                action="add_lp",
                positions=positions,
                transfers=matched_transfers
            )

            result["events"][liquidity.content_id] = liquidity
            result["transfers"] = matched_transfers

        except Exception as e:
            self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, result["errors"])

        return result

    def _handle_burn(self, log: DecodedLog, tx: Transaction) -> Dict[str, Dict]:
        result = {"transfers": {}, "events": {}, "errors": {}}
        
        try:
            amounts, bins = self._prepare_lb_amounts_and_bins(log)

            if not self._validate_attr([amounts, bins], tx.tx_hash, log.index, result["errors"]):
                return result

            unmatched_transfers = self._get_unmatched_transfers(tx)
            liq_transfers = self._get_liquidity_transfers(unmatched_transfers)

            base_withdrawals, quote_withdrawals, pool_burns, _ = self._validate_lb_liquidity_transfers(
                liq_transfers, None, False, tx.tx_hash, log.index, result["errors"]
            )
            if base_withdrawals is None:  
                return result

            router = EvmAddress(str(log.attributes.get("to")).lower())
            provider = pool_burns[0].from_address.lower()

            router_transfers = []
            if provider != router:
                base_router_transfers = [t for t in liq_transfers["underlying_transfers"].values() 
                                       if t.token == self.base_token and t.from_address == router and 
                                          t.to_address == provider and base_withdrawals and 
                                          compare_amounts(t.amount, base_withdrawals[0].amount) == 0]
                quote_router_transfers = [t for t in liq_transfers["underlying_transfers"].values() 
                                        if t.token == self.quote_token and t.from_address == router and 
                                           t.to_address == provider and quote_withdrawals and 
                                           compare_amounts(t.amount, quote_withdrawals[0].amount) == 0]
                
                router_transfer_groups = [
                    (base_router_transfers, "base_router_transfers", 1, True),
                    (quote_router_transfers, "quote_router_transfers", 1, True)
                ]
                if not self._validate_transfer_counts_flexible(router_transfer_groups, tx.tx_hash, log.index, 
                                                             "invalid_liquidity_withdrawal_transfers", result["errors"]):
                    return result
                
                router_transfers = base_router_transfers + quote_router_transfers

            positions, sum_base, sum_quote, sum_receipt = self._validate_and_create_lb_positions(
                amounts, bins, pool_burns, tx, log, False, result["errors"]
            )
            if positions is None:  
                return result

            expected_base = base_withdrawals[0].amount if base_withdrawals else "0"
            expected_quote = quote_withdrawals[0].amount if quote_withdrawals else "0"
            
            if not (compare_amounts(sum_base, expected_base) == 0 and compare_amounts(sum_quote, expected_quote) == 0 and compare_amounts(sum_receipt, pool_burns[0].amount) == 0):
                error = create_transform_error(
                    error_type="invalid_liquidity_withdrawal_transfers",
                    message="Sum of amounts in positions does not match withdrawal transfer amounts",
                    tx_hash=tx.tx_hash, log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result

            transfers_to_match = base_withdrawals + quote_withdrawals + pool_burns + router_transfers
            matched_transfers = self._handle_batch_transfers(transfers_to_match)

            liquidity = Liquidity(
                timestamp=tx.timestamp,
                tx_hash=tx.tx_hash,
                pool=log.contract,
                provider=provider,
                base_token=self.base_token,
                amount_base=amount_to_str(-amount_to_int(sum_base)),
                quote_token=self.quote_token,
                amount_quote=amount_to_str(-amount_to_int(sum_quote)),
                action="remove_lp",
                positions=positions,
                transfers=matched_transfers
            )

            result["events"][liquidity.content_id] = liquidity
            result["transfers"] = matched_transfers

        except Exception as e:
            self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, result["errors"])

        return result

    def _handle_swap(self, swap_logs: List[DecodedLog], tx: Transaction) -> Dict[str, Dict]:
        result = {"transfers": {}, "events": {}, "errors": {}}

        try:
            if not swap_logs:
                return result
            
            taker = swap_logs[0].attributes.get("to").lower()
            if not self._validate_attr([taker], tx.tx_hash, swap_logs[0].index, result["errors"]):
                return result
            
            unmatched_transfers = self._get_unmatched_transfers(tx)
            swap_transfers = self._get_swap_transfers(unmatched_transfers)

            net_base_amount, net_quote_amount, swap_batch = self._aggregate_swap_logs(swap_logs)
                
            direction = self._get_swap_direction(net_base_amount)
            
            base_swaps = [t for t in swap_transfers["base_swaps"].values() if compare_amounts(t.amount, amount_to_str(abs(amount_to_int(net_base_amount)))) == 0]
            quote_swaps = [t for t in swap_transfers["quote_swaps"].values() if compare_amounts(t.amount, amount_to_str(abs(amount_to_int(net_quote_amount)))) == 0]

            for transfers, name in [(base_swaps, "base_swaps"), (quote_swaps, "quote_swaps")]:
                if not self._validate_transfer_count(transfers, name, 1, tx.tx_hash, swap_logs[0].index, 
                                                   "invalid_swap_transfers", result["errors"]):
                    return result

            matched_transfers = self._handle_batch_transfers(base_swaps + quote_swaps)

            pool_swap = PoolSwap(
                timestamp=tx.timestamp,
                tx_hash=tx.tx_hash,
                pool=self.contract_address,
                taker=taker,
                direction=direction,
                base_token=self.base_token,
                base_amount=net_base_amount,
                quote_token=self.quote_token,
                quote_amount=net_quote_amount,
                transfers=matched_transfers,
                batch=swap_batch
            )

            result["events"][pool_swap.content_id] = pool_swap
            result["transfers"] = matched_transfers

        except Exception as e:
            error = create_transform_error(
                error_type="processing_exception",
                message=f"Exception in swap handling: {str(e)}",
                tx_hash=tx.tx_hash,
                log_index=swap_logs[0].index if swap_logs else 0,
                transformer_name=self.__class__.__name__
            )
            result["errors"][error.error_id] = error

        return result

    def process_transfers(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[Optional[Dict[DomainEventId,Transfer]],Optional[Dict[ErrorId,ProcessingError]]]:
        transfers, errors = {}, {}

        for log in logs:
            try:
                if log.name == "TransferBatch":
                    from_addr = EvmAddress(str(log.attributes.get("from")).lower())
                    to_addr = EvmAddress(str(log.attributes.get("to")).lower())
                    amounts = [amount_to_str(amt) for amt in log.attributes.get("amounts")]  
                    bins = [int(bin_id) for bin_id in log.attributes.get("ids")] 

                    if not len(amounts) == len(bins):
                        error = create_transform_error(
                            error_type= "invalid_lb_transfer",
                            message = f"LB Transfer Batch: Expected amounts and bins to have the same length, got {len(amounts)} and {len(bins)}",
                            tx_hash = tx.tx_hash,
                            log_index = log.index
                        )
                        errors[error.error_id] = error
                        continue

                    if not self._validate_attr([from_addr, to_addr, amounts, bins], tx.tx_hash, log.index, errors):
                        continue

                    transferids = {}
                    sum_transfers = 0
                    
                    for i, bin_id in enumerate(bins):   
                        amount = amounts[i]
                        transferids[bin_id] = amount
                        sum_transfers += amount_to_int(amount)

                    transfer = UnmatchedTransfer(
                        timestamp=tx.timestamp,
                        tx_hash=tx.tx_hash,
                        token=log.contract,
                        amount=amount_to_str(sum_transfers),
                        from_address=from_addr,
                        to_address=to_addr,
                        transfer_type="transfer_batch",
                        batch=transferids,
                        log_index=log.index
                    )
                    transfers[transfer.content_id] = transfer

            except Exception as e:
                self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, errors)
                
        return transfers if transfers else None, errors if errors else None

    def process_logs(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[Optional[Dict[DomainEventId,Transfer]], Optional[Dict[DomainEventId, DomainEvent]], Optional[Dict[ErrorId,ProcessingError]]]:
        new_events, matched_transfers, errors = {}, {}, {}

        try:
            swap_logs = [log for log in logs if log.name == "Swap"]

            if swap_logs:
                swap_result = self._handle_swap(swap_logs, tx)
                if swap_result:
                    new_events.update(swap_result["events"])
                    matched_transfers.update(swap_result["transfers"])
                    errors.update(swap_result["errors"])

            else:
                handler_map = {
                    "DepositedToBins": self._handle_mint,
                    "WithdrawnFromBins": self._handle_burn
                }

                for log in logs:
                    try:
                        handler = handler_map.get(log.name)
                        if handler:
                            result = handler(log, tx)
                            if result:
                                new_events.update(result["events"])
                                matched_transfers.update(result["transfers"])
                                errors.update(result["errors"])
                    except Exception as e:
                        self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, errors)


        except Exception as e:
            self._create_tx_exception(e, tx.tx_hash, self.__class__.__name__, errors)
        
        return (
            matched_transfers if matched_transfers else None, 
            new_events if new_events else None, 
            errors if errors else None
        )