from typing import List, Dict, Tuple, Optional, Any
import msgspec

from ..base import BaseTransformer
from ....types import (
    ZERO_ADDRESS,
    DecodedLog,
    Transaction,
    EvmAddress,
    DomainEvent,
    ProcessingError,
    Transfer,
    UnmatchedTransfer,
    MatchedTransfer,
    Liquidity,
    Position,
    Fee,
    PoolSwap,
    RewardSet,
    Reward,
    DomainEventId,
    ErrorId,
    create_transform_error,
    EvmHash,
)

'''
TODO: Handle the NFP Manager on Mints/Burns/Collects
'''
class PharClpoolTransformer(BaseTransformer):   
    def __init__(self, contract: EvmAddress, token0: EvmAddress, token1: EvmAddress, base_token: EvmAddress, nfp_manager: EvmAddress):
        super().__init__(contract_address=contract.lower())
        self.token0 = token0.lower()
        self.token1 = token1.lower()
        self.base_token = base_token.lower()
        self.quote_token = self.token1 if self.token0 == self.base_token else self.token0
        self.nfp_manager = nfp_manager.lower()

    def get_amounts(self, log: DecodedLog) -> tuple[Optional[int], Optional[int]]:
        """Extract base and quote amounts from log attributes"""
        try:
            amount0 = log.attributes.get("amount0")
            amount1 = log.attributes.get("amount1")

            if amount0 is None or amount1 is None:
                return None, None

            if self.token0 == self.base_token:
                return amount0, amount1
            else:
                return amount1, amount0
        except Exception:
            return None, None

    
    def _validate_attr(self, values: List[Any], tx_hash: EvmHash, log_index: int, error_dict: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate that all required attributes are present"""
        if not all(value is not None for value in values):
            error = create_transform_error(
                error_type="missing_attributes",
                message="Transformer missing required attributes in log",
                tx_hash=tx_hash,
                log_index=log_index
            )
            error_dict[error.error_id] = error
            return False
        return True
    
    def _create_log_exception(self, e, tx_hash: EvmHash, log_index: int, transformer_name: str, error_dict: Dict[ErrorId, ProcessingError]) -> None:
        """Create a ProcessingError for exceptions"""
        error = create_transform_error(
            error_type="processing_exception",
            message=f"Log processing exception: {str(e)}",
            tx_hash=tx_hash,
            log_index=log_index,
            transformer_name=transformer_name
        )
        error_dict[error.error_id] = error
        return None
    
    def _create_tx_exception(self, e, tx_hash: EvmHash, transformer_name: str, error_dict: Dict[ErrorId, ProcessingError]) -> None:
        """Create a ProcessingError for exceptions"""
        error = create_transform_error(
            error_type="processing_exception",
            message=f"Transaction processing exception: {str(e)}",
            tx_hash=tx_hash,
            transformer_name=transformer_name
        )
        error_dict[error.error_id] = error
        return None

    def _get_liquidity_transfers(self, unmatched_transfers: Dict[EvmAddress, Dict[DomainEventId, Transfer]]) -> Dict[str, Dict[DomainEventId, Transfer]]:
        """Get liquidity-related transfers (no LP tokens in CL pools, just underlying tokens)"""
        liq_transfers = {
            "deposits": {},
            "withdrawals": {},
            "underlying_transfers": {}
        }

        for contract, trf_dict in unmatched_transfers.items():
            for key, transfer in trf_dict.items():
                if transfer.token == self.base_token or transfer.token == self.quote_token:
                    if transfer.to_address == self.contract_address:
                        liq_transfers["deposits"][key] = transfer
                    elif transfer.from_address == self.contract_address:
                        liq_transfers["withdrawals"][key] = transfer
                    else:
                        liq_transfers["underlying_transfers"][key] = transfer
        
        return liq_transfers

    def _get_swap_transfers(self, unmatched_transfers: Dict[EvmAddress, Dict[DomainEventId, Transfer]]) -> Dict[str, Dict[DomainEventId, Transfer]]:
        """Get swap-related transfers"""
        swap_transfers = {
            "base_swaps": {},
            "quote_swaps": {},
        }

        for contract, trf_dict in unmatched_transfers.items():
            for key, transfer in trf_dict.items():
                if not (transfer.from_address == self.contract_address or transfer.to_address == self.contract_address):
                    continue
                
                if transfer.token == self.base_token:
                    swap_transfers["base_swaps"][key] = transfer
                elif transfer.token == self.quote_token:
                    swap_transfers["quote_swaps"][key] = transfer

        return swap_transfers

    def _handle_mint(self, log: DecodedLog, tx: Transaction) -> Dict[str, Dict]:
        result = {
            "transfers": {},
            "events": {},
            "errors": {}
        }
        
        try:
            custodian = log.attributes.get("owner")
            liquidity_amount = log.attributes.get("amount")
            base_amount, quote_amount = self.get_amounts(log)
            
            if not self._validate_attr([custodian, base_amount, quote_amount], tx.tx_hash, log.index, result["errors"]):
                return result

            unmatched_transfers = self._get_unmatched_transfers(tx)
            liq_transfers = self._get_liquidity_transfers(unmatched_transfers)
            
            base_deposits = [t for t in liq_transfers["deposits"].values() 
                           if t.token == self.base_token and t.amount == base_amount]
            quote_deposits = [t for t in liq_transfers["deposits"].values() 
                            if t.token == self.quote_token and t.amount == quote_amount]

            if base_amount > 0 and len(base_deposits) != 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_deposit",
                    message=f"Expected exactly 1 base token deposit for amount {base_amount}, found {len(base_deposits)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result
                
            if quote_amount > 0 and len(quote_deposits) != 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_deposit", 
                    message=f"Expected exactly 1 quote token deposit for amount {quote_amount}, found {len(quote_deposits)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result

            matched_transfers = {}
            if base_deposits:
                base_matched = msgspec.convert(base_deposits[0], type=MatchedTransfer)
                matched_transfers[base_matched.content_id] = base_matched
            if quote_deposits:
                quote_matched = msgspec.convert(quote_deposits[0], type=MatchedTransfer)
                matched_transfers[quote_matched.content_id] = quote_matched

            position = Position(
                timestamp=tx.timestamp,
                tx_hash=tx.tx_hash,
                receipt_token=log.contract,
                receipt_id=0,
                amount_base=base_amount,
                amount_quote=quote_amount,
                amount_receipt=liquidity_amount,
                custodian=custodian,
                log_index=log.index
            )

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
        result = {
            "transfers": {},
            "events": {},
            "errors": {}
        }
        
        try:
            custodian = log.attributes.get("owner")
            liquidity_amount = log.attributes.get("amount")
            base_amount, quote_amount = self.get_amounts(log)
            
            if not self._validate_attr([custodian, base_amount, quote_amount], tx.tx_hash, log.index, result["errors"]):
                return result

            unmatched_transfers = self._get_unmatched_transfers(tx)
            liq_transfers = self._get_liquidity_transfers(unmatched_transfers)
            
            base_withdrawals = [t for t in liq_transfers["withdrawals"].values() 
                              if t.token == self.base_token and t.amount == base_amount and t.to_address == custodian]
            quote_withdrawals = [t for t in liq_transfers["withdrawals"].values() 
                               if t.token == self.quote_token and t.amount == quote_amount and t.to_address == custodian]

            if base_amount > 0 and len(base_withdrawals) != 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_withdrawal",
                    message=f"Expected exactly 1 base token withdrawal for amount {base_amount}, found {len(base_withdrawals)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result
                
            if quote_amount > 0 and len(quote_withdrawals) != 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_withdrawal", 
                    message=f"Expected exactly 1 quote token withdrawal for amount {quote_amount}, found {len(quote_withdrawals)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result

            matched_transfers = {}
            if base_withdrawals:
                base_matched = msgspec.convert(base_withdrawals[0], type=MatchedTransfer)
                matched_transfers[base_matched.content_id] = base_matched
            if quote_withdrawals:
                quote_matched = msgspec.convert(quote_withdrawals[0], type=MatchedTransfer)
                matched_transfers[quote_matched.content_id] = quote_matched

            position = Position(
                timestamp=tx.timestamp,
                tx_hash=tx.tx_hash,
                receipt_token=log.contract,
                receipt_id=0,
                amount_base=-base_amount,
                amount_quote=-quote_amount,
                amount_receipt=-liquidity_amount,
                custodian=custodian,
            )

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
        result = {
            "transfers": {},
            "events": {},
            "errors": {}
        }
        
        try:
            taker = log.attributes.get("recipient")
            base_amount, quote_amount = self.get_amounts(log)
            direction = "buy" if base_amount > 0 else "sell"
            
            if not self._validate_attr([taker, base_amount, quote_amount], tx.tx_hash, log.index, result["errors"]):
                return result

            unmatched_transfers = self._get_unmatched_transfers(tx)
            swap_transfers = self._get_swap_transfers(unmatched_transfers)

            base_swaps = [t for t in swap_transfers["base_swaps"].values() if t.amount == abs(base_amount)]
            quote_swaps = [t for t in swap_transfers["quote_swaps"].values() if t.amount == abs(quote_amount)]

            if len(base_swaps) != 1:
                error = create_transform_error(
                    error_type="invalid_swap",
                    message=f"Expected exactly 1 base token swap transfer, found {len(base_swaps)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result

            if len(quote_swaps) != 1:
                error = create_transform_error(
                    error_type="invalid_swap",
                    message=f"Expected exactly 1 quote token swap transfer, found {len(quote_swaps)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result

            base_matched = msgspec.convert(base_swaps[0], type=MatchedTransfer)
            quote_matched = msgspec.convert(quote_swaps[0], type=MatchedTransfer)
            matched_transfers = {
                base_matched.content_id: base_matched,
                quote_matched.content_id: quote_matched
            }

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
        result = {
            "transfers": {},
            "events": {},
            "errors": {}
        }
        
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

            matched_transfers = {}
            rewards = {}

            if base_amount > 0:
                if len(base_collections) != 1:
                    error = create_transform_error(
                        error_type="invalid_fee_collection",
                        message=f"Expected exactly 1 base token fee collection, found {len(base_collections)}",
                        tx_hash=tx.tx_hash,
                        log_index=log.index
                    )
                    result["errors"][error.error_id] = error
                    return result

                base_matched = msgspec.convert(base_collections[0], type=MatchedTransfer)
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
                if len(quote_collections) != 1:
                    error = create_transform_error(
                        error_type="invalid_fee_collection",
                        message=f"Expected exactly 1 quote token fee collection, found {len(quote_collections)}",
                        tx_hash=tx.tx_hash,
                        log_index=log.index
                    )
                    result["errors"][error.error_id] = error
                    return result

                quote_matched = msgspec.convert(quote_collections[0], type=MatchedTransfer)
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
        """Process Transfer events - CL pools only use standard ERC20 Transfer events for underlying tokens"""
        transfers = {}
        errors = {}

        for log in logs:
            try:
                if log.name == "Transfer":
                    from_addr = log.attributes.get("from")
                    to_addr = log.attributes.get("to")
                    value = log.attributes.get("value")
                    
                    if not self._validate_attr([from_addr, to_addr, value], tx.tx_hash, log.index, errors):
                        continue

                    transfer = UnmatchedTransfer(
                        timestamp=tx.timestamp,
                        tx_hash=tx.tx_hash,
                        from_address=from_addr.lower(),
                        to_address=to_addr.lower(),
                        token=log.contract,
                        amount=value,
                        log_index=log.index
                    )
                    transfers[transfer.content_id] = transfer

            except Exception as e:
                self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, errors)
                
        return transfers if transfers else None, errors if errors else None

    def process_logs(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[Optional[Dict[DomainEventId, Transfer]], Optional[Dict[DomainEventId, DomainEvent]], Optional[Dict[ErrorId, ProcessingError]]]:
        new_events, matched_transfers, errors = {}, {}, {}

        try:
            for log in logs:
                try:
                    if log.name == "Swap":
                        swap_result = self._handle_swap(log, tx)
                        if swap_result:
                            new_events.update(swap_result["events"])
                            matched_transfers.update(swap_result["transfers"])
                            errors.update(swap_result["errors"])

                    elif log.name == "Mint":
                        mint_result = self._handle_mint(log, tx)
                        if mint_result:
                            new_events.update(mint_result["events"])
                            matched_transfers.update(mint_result["transfers"])
                            errors.update(mint_result["errors"])

                    elif log.name == "Burn":
                        burn_result = self._handle_burn(log, tx)
                        if burn_result:
                            new_events.update(burn_result["events"])
                            matched_transfers.update(burn_result["transfers"])
                            errors.update(burn_result["errors"])

                    elif log.name == "Collect":
                        collect_result = self._handle_collect(log, tx)
                        if collect_result:
                            new_events.update(collect_result["events"])
                            matched_transfers.update(collect_result["transfers"])
                            errors.update(collect_result["errors"])
                
                except Exception as e:
                    self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, errors)

        except Exception as e:
            self._create_tx_exception(e, tx.tx_hash, self.__class__.__name__, errors)
        
        return (
            matched_transfers if matched_transfers else None, 
            new_events if new_events else None, 
            errors if errors else None
        )