# indexer/transform/transformers/pools/phar_clpool.py

from typing import List, Dict, Tuple, Optional

from ..indexer.transform.transformers.pools.pool_base import PoolTransformer
from ..indexer.types import (
    DecodedLog,
    Transaction,
    EvmAddress,
    DomainEvent,
    ProcessingError,
    Transfer,
    Liquidity,
    Position,
    PoolSwap,
    RewardSet,
    Reward,
    DomainEventId,
    ErrorId,
    create_transform_error,
)

'''
TODO: Handle the NFP Manager on Mints/Burns/Collects
'''

class PharClPoolTransformer(PoolTransformer):   
    def __init__(self, contract: EvmAddress, token0: EvmAddress, token1: EvmAddress, base_token: EvmAddress, nfp_manager: EvmAddress):
        super().__init__(contract, token0, token1, base_token, fee_collector=None)
        self.nfp_manager = nfp_manager.lower()

    def _validate_cl_liquidity_transfers(self, liq_transfers: Dict, custodian: EvmAddress, 
                                       base_amount: int, quote_amount: int, is_mint: bool,
                                       tx_hash: str, log_index: int, error_dict: Dict) -> Tuple[List, List]:
        if is_mint:
            base_deposits = [t for t in liq_transfers["deposits"].values() 
                           if t.token == self.base_token and t.amount == base_amount] if base_amount > 0 else []
            quote_deposits = [t for t in liq_transfers["deposits"].values() 
                            if t.token == self.quote_token and t.amount == quote_amount] if quote_amount > 0 else []
            
            if base_amount > 0 and len(base_deposits) != 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_deposit_transfers",
                    message=f"Expected exactly 1 base token deposit for amount {base_amount}, found {len(base_deposits)}",
                    tx_hash=tx_hash, log_index=log_index
                )
                error_dict[error.error_id] = error
                return None, None
                
            if quote_amount > 0 and len(quote_deposits) != 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_deposit_transfers", 
                    message=f"Expected exactly 1 quote token deposit for amount {quote_amount}, found {len(quote_deposits)}",
                    tx_hash=tx_hash, log_index=log_index
                )
                error_dict[error.error_id] = error
                return None, None

            return base_deposits, quote_deposits
        else:
            base_withdrawals = [t for t in liq_transfers["withdrawals"].values() 
                              if t.token == self.base_token and t.amount == base_amount and t.to_address == custodian] if base_amount > 0 else []
            quote_withdrawals = [t for t in liq_transfers["withdrawals"].values() 
                               if t.token == self.quote_token and t.amount == quote_amount and t.to_address == custodian] if quote_amount > 0 else []

            if base_amount > 0 and len(base_withdrawals) != 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_withdrawal_transfers",
                    message=f"Expected exactly 1 base token withdrawal for amount {base_amount}, found {len(base_withdrawals)}",
                    tx_hash=tx_hash, log_index=log_index
                )
                error_dict[error.error_id] = error
                return None, None
                
            if quote_amount > 0 and len(quote_withdrawals) != 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_withdrawal_transfers", 
                    message=f"Expected exactly 1 quote token withdrawal for amount {quote_amount}, found {len(quote_withdrawals)}",
                    tx_hash=tx_hash, log_index=log_index
                )
                error_dict[error.error_id] = error
                return None, None

            return base_withdrawals, quote_withdrawals

    def _create_cl_position(self, tx: Transaction, log: DecodedLog, custodian: EvmAddress,
                          base_amount: int, quote_amount: int, liquidity_amount: int, 
                          is_mint: bool) -> Position:
        multiplier = 1 if is_mint else -1
        return Position(
            timestamp=tx.timestamp,
            tx_hash=tx.tx_hash,
            receipt_token=log.contract,
            receipt_id=0,
            amount_base=base_amount * multiplier,
            amount_quote=quote_amount * multiplier,
            amount_receipt=liquidity_amount * multiplier,
            custodian=custodian,
            log_index=log.index
        )

    def _handle_mint(self, log: DecodedLog, tx: Transaction) -> Dict[str, Dict]:
        result = {"transfers": {}, "events": {}, "errors": {}}
        
        try:
            custodian = log.attributes.get("owner")
            liquidity_amount = log.attributes.get("amount")
            base_amount, quote_amount = self.get_amounts(log)
            
            if not self._validate_attr([custodian, base_amount, quote_amount], tx.tx_hash, log.index, result["errors"]):
                return result

            unmatched_transfers = self._get_unmatched_transfers(tx)
            liq_transfers = self._get_liquidity_transfers(unmatched_transfers)

            base_deposits, quote_deposits = self._validate_cl_liquidity_transfers(
                liq_transfers, custodian, base_amount, quote_amount, True, tx.tx_hash, log.index, result["errors"]
            )
            if base_deposits is None:  # Validation failed
                return result

            transfers_to_match = base_deposits + quote_deposits
            matched_transfers = self._create_matched_transfers_dict(transfers_to_match)

            position = self._create_cl_position(tx, log, custodian, base_amount, quote_amount, liquidity_amount, True)

            liquidity = Liquidity(
                timestamp=tx.timestamp,
                tx_hash=tx.tx_hash,
                pool=log.contract,
                provider=custodian,
                base_token=self.base_token,
                amount_base=base_amount,
                quote_token=self.quote_token,
                amount_quote=quote_amount,
                action="add_lp",
                positions={position.content_id: position},
                transfers=matched_transfers,
                log_index=log.index
            )

            result["events"][liquidity.content_id] = liquidity
            result["transfers"] = matched_transfers

        except Exception as e:
            self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, result["errors"])

        return result

    def _handle_burn(self, log: DecodedLog, tx: Transaction) -> Dict[str, Dict]:
        result = {"transfers": {}, "events": {}, "errors": {}}
        
        try:
            custodian = log.attributes.get("owner")
            liquidity_amount = log.attributes.get("amount")
            base_amount, quote_amount = self.get_amounts(log)
            
            if not self._validate_attr([custodian, base_amount, quote_amount], tx.tx_hash, log.index, result["errors"]):
                return result

            unmatched_transfers = self._get_unmatched_transfers(tx)
            liq_transfers = self._get_liquidity_transfers(unmatched_transfers)

            base_withdrawals, quote_withdrawals = self._validate_cl_liquidity_transfers(
                liq_transfers, custodian, base_amount, quote_amount, False, tx.tx_hash, log.index, result["errors"]
            )
            if base_withdrawals is None: 
                return result

            transfers_to_match = base_withdrawals + quote_withdrawals
            matched_transfers = self._create_matched_transfers_dict(transfers_to_match)

            position = self._create_cl_position(tx, log, custodian, base_amount, quote_amount, liquidity_amount, False)

            liquidity = Liquidity(
                timestamp=tx.timestamp,
                tx_hash=tx.tx_hash,
                pool=log.contract,
                provider=custodian,
                base_token=self.base_token,
                amount_base=-base_amount,
                quote_token=self.quote_token,
                amount_quote=-quote_amount,
                action="remove_lp",
                positions={position.content_id: position},
                transfers=matched_transfers,
            )

            result["events"][liquidity.content_id] = liquidity
            result["transfers"] = matched_transfers

        except Exception as e:
            self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, result["errors"])

        return result

    def _handle_swap(self, log: DecodedLog, tx: Transaction) -> Dict[str, Dict]:
        result = {"transfers": {}, "events": {}, "errors": {}}
        
        try:
            taker = log.attributes.get("recipient")
            base_amount, quote_amount = self.get_amounts(log)
            direction = self._get_swap_direction(base_amount)
            
            if not self._validate_attr([taker, base_amount, quote_amount], tx.tx_hash, log.index, result["errors"]):
                return result

            unmatched_transfers = self._get_unmatched_transfers(tx)
            swap_transfers = self._get_swap_transfers(unmatched_transfers)

            base_swaps = [t for t in swap_transfers["base_swaps"].values() if t.amount == abs(base_amount)]
            quote_swaps = [t for t in swap_transfers["quote_swaps"].values() if t.amount == abs(quote_amount)]

            for transfers, name in [(base_swaps, "base_swaps"), (quote_swaps, "quote_swaps")]:
                if not self._validate_transfer_count(transfers, name, 1, tx.tx_hash, log.index, 
                                                   "invalid_swap_transfers", result["errors"]):
                    return result

            matched_transfers = self._create_matched_transfers_dict(base_swaps + quote_swaps)

            pool_swap = PoolSwap(
                timestamp=tx.timestamp,
                tx_hash=tx.tx_hash,
                pool=log.contract,
                taker=taker,
                direction=direction,
                base_token=self.base_token,
                base_amount=base_amount,
                quote_token=self.quote_token,
                quote_amount=quote_amount,
                transfers=matched_transfers,
                log_index=log.index
            )

            result["events"][pool_swap.content_id] = pool_swap
            result["transfers"] = matched_transfers

        except Exception as e:
            self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, result["errors"])

        return result

    def _handle_collect(self, log: DecodedLog, tx: Transaction) -> Dict[str, Dict]:
        result = {"transfers": {}, "events": {}, "errors": {}}
        
        try:
            owner = log.attributes.get("owner")
            recipient = log.attributes.get("recipient", owner)
            base_amount, quote_amount = self.get_amounts(log)

            unmatched_transfers = self._get_unmatched_transfers(tx)
            liq_transfers = self._get_liquidity_transfers(unmatched_transfers)

            base_collections = [t for t in liq_transfers["withdrawals"].values() 
                              if t.token == self.base_token and t.amount == base_amount and t.to_address == recipient] if base_amount > 0 else []
            quote_collections = [t for t in liq_transfers["withdrawals"].values() 
                               if t.token == self.quote_token and t.amount == quote_amount and t.to_address == recipient] if quote_amount > 0 else []

            matched_transfers, rewards = {}, {}

            if base_amount > 0:
                if not self._validate_transfer_count(base_collections, "base_collections", 1, tx.tx_hash, log.index, 
                                                   "invalid_fee_collection_transfers", result["errors"]):
                    return result

                base_matched = self._convert_to_matched_transfer(base_collections[0])
                matched_transfers[base_matched.content_id] = base_matched

                base_reward = Reward(
                    timestamp=tx.timestamp,
                    tx_hash=tx.tx_hash,
                    reward_token=self.base_token,
                    amount=base_amount,
                    reward_type="claim_fees"
                )
                reward_set = RewardSet(
                    timestamp=tx.timestamp,
                    tx_hash=tx.tx_hash,
                    contract=log.contract,
                    recipient=recipient,
                    token=self.base_token,
                    amount=base_amount,
                    rewards={base_reward.content_id: base_reward},
                    log_index=log.index
                )
                rewards[reward_set.content_id] = reward_set

            if quote_amount > 0:
                if not self._validate_transfer_count(quote_collections, "quote_collections", 1, tx.tx_hash, log.index, 
                                                   "invalid_fee_collection_transfers", result["errors"]):
                    return result

                quote_matched = self._convert_to_matched_transfer(quote_collections[0])
                matched_transfers[quote_matched.content_id] = quote_matched

                quote_reward = Reward(
                    timestamp=tx.timestamp,
                    tx_hash=tx.tx_hash,
                    reward_token=self.quote_token,
                    amount=quote_amount,
                    reward_type="claim_fees"
                )

                reward_set = RewardSet(
                    timestamp=tx.timestamp,
                    tx_hash=tx.tx_hash,
                    contract=log.contract,
                    recipient=recipient,
                    token=self.quote_token,
                    amount=quote_amount,
                    rewards={quote_reward.content_id: quote_reward},
                    log_index=log.index
                )
                rewards[reward_set.content_id] = reward_set

            if rewards:
                result["events"] = rewards
                result["transfers"] = matched_transfers

        except Exception as e:
            self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, result["errors"])

        return result

    def process_transfers(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[Optional[Dict[DomainEventId, Transfer]], Optional[Dict[ErrorId, ProcessingError]]]:
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

    def process_logs(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[Optional[Dict[DomainEventId, Transfer]], Optional[Dict[DomainEventId, DomainEvent]], Optional[Dict[ErrorId, ProcessingError]]]:
        new_events, matched_transfers, errors = {}, {}, {}

        try:
            handler_map = {
                "Swap": self._handle_swap,
                "Mint": self._handle_mint,
                "Burn": self._handle_burn,
                "Collect": self._handle_collect
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