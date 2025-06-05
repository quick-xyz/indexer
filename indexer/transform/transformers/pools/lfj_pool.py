# indexer/transform/transformers/pools/lfj_pool.py

from typing import List, Dict, Tuple, Optional
import msgspec

from .pool_base import PoolTransformer
from ....types import (
    DecodedLog,
    Transaction,
    EvmAddress,
    DomainEvent,
    ProcessingError,
    Transfer,
    MatchedTransfer,
    Liquidity,
    Position,
    PoolSwap,
    DomainEventId,
    ErrorId,
    create_transform_error,
    TransferLedger,
)
from ....utils.amounts import amount_to_str, amount_to_int, compare_amounts


class LfjPoolTransformer(PoolTransformer):
    def __init__(self, contract: str, token0: str, token1: str, base_token: str, fee_collector: str):
        super().__init__(contract,token0,token1,base_token,fee_collector)

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
       
    def _handle_mint(self, log: DecodedLog, tx: Transaction) -> Dict[str, Dict]:
        result = {"transfers": {}, "events": {}, "errors": {}}
        
        try:
            base_amount, quote_amount = self.get_amounts(log)
            if not self._validate_attr([base_amount, quote_amount], tx.tx_hash, log.index, result["errors"]):
                return result
            
            unmatched_transfers = self._get_unmatched_transfers(tx)
            liq_transfers = self._get_liquidity_transfers(unmatched_transfers)
            
            base_deposits = [t for t in liq_transfers["deposits"].values() 
                           if t.token == self.base_token and compare_amounts(t.amount, base_amount) == 0]
            quote_deposits = [t for t in liq_transfers["deposits"].values() 
                            if t.token == self.quote_token and compare_amounts(t.amount, quote_amount) == 0]
            pool_mints = [t for t in liq_transfers["mints"].values() 
                         if t.to_address != self.fee_collector]
            fee_collection = [t for t in liq_transfers["mints"].values() 
                            if t.to_address == self.fee_collector]

            for transfers, name, expected in [(base_deposits, "base_deposits", 1), 
                                            (quote_deposits, "quote_deposits",  1),
                                            (pool_mints, "pool_mints",  1)
                                            ]:
                if not self._validate_transfer_count(transfers, name, expected, tx.tx_hash, log.index, 
                                                   "invalid_liquidity_deposit_transfers", result["errors"]):
                    return result
            
            provider = pool_mints[0].to_address.lower()
            transfers_to_match = base_deposits + quote_deposits + pool_mints + fee_collection
            matched_transfers = self._create_matched_transfers_dict(transfers_to_match)

            position = Position(
                timestamp=tx.timestamp,
                tx_hash=tx.tx_hash,
                receipt_token=log.contract,
                receipt_id=0,
                amount_base=base_amount,
                amount_quote=quote_amount,
                amount_receipt=pool_mints[0].amount
            )

            liquidity = Liquidity(
                timestamp=tx.timestamp,
                tx_hash=tx.tx_hash,
                pool=log.contract,
                provider=provider,
                base_token=self.base_token,
                amount_base=base_amount,
                quote_token=self.quote_token,
                amount_quote=quote_amount,
                action="add_lp",
                positions={position.content_id: position},
                transfers=matched_transfers
            )
            result["events"][liquidity.content_id] = liquidity

            fee_event = self._create_fee_collection_event(fee_collection, tx)
            if fee_event:
                result["events"][fee_event.content_id] = fee_event
            
            result["transfers"] = matched_transfers

        except Exception as e:
            self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, result["errors"])

        return result
    
    def _handle_burn(self, log: DecodedLog, tx: Transaction) -> Dict[str, Dict]:
        result = {"transfers": {}, "events": {}, "errors": {}}
        
        try:
            base_amount, quote_amount = self.get_amounts(log)
            if not self._validate_attr([base_amount, quote_amount], tx.tx_hash, log.index, result["errors"]):
                return result
            
            unmatched_transfers = self._get_unmatched_transfers(tx)
            liq_transfers = self._get_liquidity_transfers(unmatched_transfers)
            
            base_withdrawals = [t for t in liq_transfers["withdrawals"].values() 
                              if t.token == self.base_token and compare_amounts(t.amount, base_amount) == 0]
            quote_withdrawals = [t for t in liq_transfers["withdrawals"].values() 
                               if t.token == self.quote_token and compare_amounts(t.amount, quote_amount) == 0]
            pool_burns = list(liq_transfers["burns"].values())
            fee_collection = [t for t in liq_transfers["mints"].values() 
                            if t.to_address == self.fee_collector]

            for transfers, name, expected in [(base_withdrawals, "base_withdrawals", 1), 
                                            (quote_withdrawals, "quote_withdrawals", 1),
                                            (pool_burns, "pool_burns", 1)]:
                if not self._validate_transfer_count(transfers, name, expected, tx.tx_hash, log.index, 
                                                   "invalid_liquidity_withdrawal_transfers", result["errors"]):
                    return result

            provider = base_withdrawals[0].to_address.lower()

            receipt_transfers = []
            if pool_burns[0].from_address != provider:
                receipt_transfers = [t for t in liq_transfers["receipt_transfers"].values() 
                                   if compare_amounts(t.amount, pool_burns[0].amount) == 0 and t.from_address == provider]
                if receipt_transfers and len(receipt_transfers) != 1:
                    error = create_transform_error(
                        error_type="invalid_liquidity_withdrawal",
                        message=f"Expected exactly 1 receipt transfer from provider, found {len(receipt_transfers)}",
                        tx_hash=tx.tx_hash, log_index=log.index
                    )
                    result["errors"][error.error_id] = error
                    return result

            transfers_to_match = base_withdrawals + quote_withdrawals + pool_burns + receipt_transfers + fee_collection
            matched_transfers = self._create_matched_transfers_dict(transfers_to_match)

            position = Position(
                timestamp=tx.timestamp,
                tx_hash=tx.tx_hash,
                receipt_token=log.contract,
                receipt_id=0,
                amount_base=amount_to_str(-amount_to_int(base_amount)),
                amount_quote=amount_to_str(-amount_to_int(quote_amount)),
                amount_receipt=amount_to_str(-amount_to_int(pool_burns[0].amount))
            )

            liquidity = Liquidity(
                timestamp=tx.timestamp,
                tx_hash=tx.tx_hash,
                pool=log.contract,
                provider=provider,
                base_token=self.base_token,
                amount_base=amount_to_str(-amount_to_int(base_amount)),
                quote_token=self.quote_token,
                amount_quote=amount_to_str(-amount_to_int(quote_amount)),
                action="remove_lp",
                positions={position.content_id: position},
                transfers=matched_transfers
            )
            
            result["events"][liquidity.content_id] = liquidity

            fee_event = self._create_fee_collection_event(fee_collection, tx)
            if fee_event:
                result["events"][fee_event.content_id] = fee_event
            
            result["transfers"] = matched_transfers

        except Exception as e:
            self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, result["errors"])

        return result

    def _handle_swap(self, log: DecodedLog, tx: Transaction) -> Dict[str, Dict]:
        result = {"transfers": {}, "events": {}, "errors": {}}
        
        try:
            base_amount, quote_amount = self.get_in_out_amounts(log)
            taker = EvmAddress(str(log.attributes.get("to")).lower())
            if not self._validate_attr([taker, base_amount, quote_amount], tx.tx_hash, log.index, result["errors"]):
                return result

            direction = self._get_swap_direction(base_amount)
            unmatched_transfers = self._get_unmatched_transfers(tx)
            swap_transfers = self._get_swap_transfers(unmatched_transfers)
            
            base_swaps = [t for t in swap_transfers["base_swaps"].values() if compare_amounts(t.amount, amount_to_str(abs(amount_to_int(base_amount)))) == 0]
            quote_swaps = [t for t in swap_transfers["quote_swaps"].values() if compare_amounts(t.amount, amount_to_str(abs(amount_to_int(quote_amount)))) == 0]

            for transfers, name in [(base_swaps, "base_swaps"), (quote_swaps, "quote_swaps")]:
                if not self._validate_transfer_count(transfers, name, 1, tx.tx_hash, log.index, 
                                                   "invalid_swap_transfers", result["errors"]):
                    return result

            matched_transfers = self._create_matched_transfers_dict(base_swaps + quote_swaps)

            swap = PoolSwap(
                timestamp=tx.timestamp,
                tx_hash=tx.tx_hash,
                pool=log.contract,
                taker=taker,
                direction=direction,
                base_token=self.base_token,
                base_amount=base_amount,
                quote_token=self.quote_token,
                quote_amount=quote_amount,
                transfers=matched_transfers
            )
            result["events"][swap.content_id] = swap
            result["transfers"] = matched_transfers

        except Exception as e:
            self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, result["errors"])

        return result

    def process_transfers(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[Optional[Dict[DomainEventId,Transfer]],Optional[Dict[ErrorId,ProcessingError]]]:
        transfers, errors = {}, {}

        for log in logs:
            try:
                if log.name == "Transfer":
                    transfer = self._build_transfer_from_log(log, tx)
                    if transfer:
                        transfers[transfer.content_id] = transfer
            except Exception as e:
                self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, errors)
                
        return transfers if transfers else None, errors if errors else None
    
    def process_logs(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[Optional[Dict[DomainEventId,Transfer]], Optional[Dict[DomainEventId, DomainEvent]], Optional[Dict[ErrorId,ProcessingError]]]:
        new_events, matched_transfers, errors = {}, {}, {}

        try:
            handler_map = {
                "Swap": self._handle_swap,
                "Mint": self._handle_mint,
                "Burn": self._handle_burn
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