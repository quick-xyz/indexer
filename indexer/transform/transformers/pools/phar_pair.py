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
    DomainEventId,
    ErrorId,
    create_transform_error,
    EvmHash,
    TransferLedger,
)

'''
Pharaoh Pairs have a Fee event and what looks like unused Claim events. Unsure what is happening with fees, but otherwise looks compatible with TJv1 pairs.
'''

class PharPairTransformer(BaseTransformer):
    def __init__(self, contract: EvmAddress, token0: EvmAddress, token1: EvmAddress, base_token: EvmAddress, fee_collector: EvmAddress):
        super().__init__(contract_address=contract.lower())
        self.token0 = token0.lower()
        self.token1 = token1.lower()
        self.base_token = base_token.lower()
        self.quote_token = self.token1 if self.token0 == self.base_token else self.token0
        self.fee_collector = fee_collector.lower()

    
    def get_amounts(self, log: DecodedLog) -> tuple[Optional[int], Optional[int]]:
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

    def get_in_out_amounts(self, log: DecodedLog) -> tuple[Optional[int], Optional[int]]:
        try:            
            amount0 = log.attributes.get("amount0In") - log.attributes.get("amount0Out")
            amount1 = log.attributes.get("amount1In") - log.attributes.get("amount1Out")
            
            if self.token0 == self.base_token:
                return amount0, amount1
            else:
                return amount1, amount0
        except Exception:
            return None, None
            
    def get_direction(self, base_amount: int) -> str:
        return "buy" if base_amount > 0 else "sell"
    
    def _validate_attr(self, values: List[Any],tx_hash: EvmHash, log_index: int, error_dict: Dict[ErrorId,ProcessingError]) -> bool:
        """ Validate that all required attributes are present """
        if not all(value is not None for value in values):
            error = create_transform_error(
                error_type="missing_attributes",
                message=f"Transformer missing required attributes in log",
                tx_hash=tx_hash,
                log_index=log_index
            )
            error_dict[error.error_id] = error
            return False
        return True
    
    def _create_log_exception(self, e, tx_hash: EvmHash, log_index: int, transformer_name: str, error_dict: Dict[ErrorId,ProcessingError]) -> None:
        """ Create a ProcessingError for exceptions """
        error = create_transform_error(
            error_type="processing_exception",
            message=f"Log processing exception: {str(e)}",
            tx_hash=tx_hash,
            log_index=log_index,
            transformer_name=transformer_name
        )
        error_dict[error.error_id] = error
        return None
    
    def _create_tx_exception(self, e, tx_hash: EvmHash, transformer_name: str, error_dict: Dict[ErrorId,ProcessingError]) -> None:
        """ Create a ProcessingError for exceptions """
        error = create_transform_error(
            error_type="processing_exception",
            message=f"Transaction processing exception: {str(e)}",
            tx_hash=tx_hash,
            transformer_name=transformer_name
        )
        error_dict[error.error_id] = error
        return None

    def _get_swap_transfers(self, unmatched_transfers: Dict[EvmAddress,Dict[DomainEventId, Transfer]]) -> Dict[str, Dict[DomainEventId,Transfer]]:
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

    def _get_liquidity_transfers(self, unmatched_transfers: Dict[EvmAddress,Dict[DomainEventId, Transfer]]) -> Dict[str, Dict[DomainEventId,Transfer]]:
        liq_transfers = {
            "mints": {},
            "burns": {},
            "deposits": {},
            "withdrawals": {},
            "underlying_transfers": {},
            "receipt_transfers": {}
        }

        for contract, trf_dict in unmatched_transfers.items():
            for key, transfer in trf_dict.items():
                if transfer.token == self.contract_address:
                    if transfer.from_address == ZERO_ADDRESS:
                        liq_transfers["mints"][key] = transfer
                    elif transfer.to_address == ZERO_ADDRESS:
                        liq_transfers["burns"][key] = transfer
                    else:
                        liq_transfers["receipt_transfers"][key] = transfer

                elif transfer.token == self.base_token or transfer.token == self.quote_token:
                    if transfer.to_address == self.contract_address:
                        liq_transfers["deposits"][key] = transfer
                    elif transfer.from_address == self.contract_address:
                        liq_transfers["withdrawals"][key] = transfer
                    else:
                        liq_transfers["underlying_transfers"][key] = transfer
        return liq_transfers
       
    def _handle_mint(self, log: DecodedLog, tx: Transaction) -> Dict[str, Dict]:
        result = {
            "transfers": {},
            "events": {},
            "errors": {}
        }
        
        try:
            base_amount, quote_amount = self.get_amounts(log)
            if not self._validate_attr([base_amount, quote_amount], tx.tx_hash, log.index, result["errors"]):
                return result
            
            unmatched_transfers = self._get_unmatched_transfers(tx)
            liq_transfers = self._get_liquidity_transfers(unmatched_transfers)
            
            base_deposits = [t for t in liq_transfers["deposits"].values() if t.token == self.base_token and t.amount == base_amount]
            quote_deposits = [t for t in liq_transfers["deposits"].values() if t.token == self.quote_token and t.amount == quote_amount]
            pool_mints = [t for t in liq_transfers["mints"].values() if t.to_address != self.fee_collector]

            if len(base_deposits) != 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_deposit",
                    message=f"Expected exactly 1 base token deposit, found {len(base_deposits)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                if not error.context:
                    error.context = {}
                error.context['log_index'] = log.index
                result["errors"][error.error_id] = error
                return result
                
            if len(quote_deposits) != 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_deposit", 
                    message=f"Expected exactly 1 quote token deposit, found {len(quote_deposits)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result
                
            if len(pool_mints) != 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_deposit",
                    message=f"Expected exactly 1 receipt token mint, found {len(pool_mints)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result
            
            provider = pool_mints[0].to_address.lower()
            fee_collection = [t for t in liq_transfers["mints"].values() if t.to_address == self.fee_collector]
            
            matched_transfers = {}          
            positions = {}  
            collection = {}       

            base_matched = msgspec.convert(base_deposits[0], type=MatchedTransfer)            
            quote_matched = msgspec.convert(quote_deposits[0], type=MatchedTransfer)           
            pool_matched = msgspec.convert(pool_mints[0], type=MatchedTransfer)
            
            matched_transfers = {
                base_matched.content_id: base_matched,
                quote_matched.content_id: quote_matched,
                pool_matched.content_id: pool_matched
            }

            position = Position(
                timestamp=tx.timestamp,
                tx_hash=tx.tx_hash,
                receipt_token=log.contract,
                receipt_id=0,
                amount_base=base_amount,
                amount_quote=quote_amount,
                amount_receipt=pool_mints[0].amount
            )
            positions[position.content_id] = position

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
                positions=positions,
                transfers=matched_transfers
            )

            result["events"][liquidity.content_id] = liquidity

            fee_collection_matched = msgspec.convert(fee_collection[0], type=MatchedTransfer) if fee_collection else None

            if fee_collection_matched:
                collection[fee_collection_matched.content_id] = fee_collection_matched
                matched_transfers[fee_collection_matched.content_id] = fee_collection_matched

                fee_received = TransferLedger(
                    timestamp=tx.timestamp,
                    tx_hash=tx.tx_hash,
                    token=log.contract,
                    address=fee_collection_matched.to_address.lower(),
                    amount=fee_collection_matched.amount,
                    action="received",
                    transfers=collection,
                    desc="Protocol fees collected",
                )

                result["events"][fee_received.content_id] = fee_received
            
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
            base_amount, quote_amount = self.get_amounts(log)
            if not self._validate_attr([base_amount, quote_amount], tx.tx_hash, log.index, result["errors"]):
                return result
            
            unmatched_transfers = self._get_unmatched_transfers(tx)
            liq_transfers = self._get_liquidity_transfers(unmatched_transfers)
            
            base_withdrawals = [t for t in liq_transfers["withdrawals"].values() if t.token == self.base_token and t.amount == base_amount]
            quote_withdrawals = [t for t in liq_transfers["withdrawals"].values() if t.token == self.quote_token and t.amount == quote_amount]
            pool_burns = [t for t in liq_transfers["burns"].values()]

            if len(base_withdrawals) != 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_withdrawal",
                    message=f"Expected exactly 1 base token withdrawal, found {len(base_withdrawals)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result
                
            if len(quote_withdrawals) != 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_withdrawal", 
                    message=f"Expected exactly 1 quote token withdrawal, found {len(quote_withdrawals)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result
                
            if len(pool_burns) != 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_withdrawal",
                    message=f"Expected exactly 1 receipt token burn, found {len(pool_burns)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result
            
            provider = base_withdrawals[0].to_address.lower()
            fee_collection = [t for t in liq_transfers["mints"].values() if t.to_address == self.fee_collector]

            burn_amount = pool_burns[0].amount
            if pool_burns[0].from_address != provider:
                trf_receipts = [t for t in liq_transfers["receipt_transfers"].values() if t.amount == burn_amount and t.from_address == provider]
            
            matched_transfers = {}   
            positions = {} 
            collection = {}       

            base_matched = msgspec.convert(base_withdrawals[0], type=MatchedTransfer)            
            quote_matched = msgspec.convert(quote_withdrawals[0], type=MatchedTransfer)           
            pool_matched = msgspec.convert(pool_burns[0], type=MatchedTransfer)
            
            matched_transfers = {
                base_matched.content_id: base_matched,
                quote_matched.content_id: quote_matched,
                pool_matched.content_id: pool_matched
            }

            if trf_receipts and len(trf_receipts) == 1:
                trf_burn_matched = msgspec.convert(trf_receipts[0], type=MatchedTransfer)
                matched_transfers[trf_burn_matched.content_id] = trf_burn_matched
            elif trf_receipts:
                error = create_transform_error(
                    error_type="invalid_liquidity_withdrawal",
                    message=f"Expected exactly 1 receipt transfer from provider, found {len(trf_receipts)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result

            # Note: Burns return negative amounts for Position and Liquidity

            position = Position(
                timestamp=tx.timestamp,
                tx_hash=tx.tx_hash,
                receipt_token=log.contract,
                receipt_id=0,
                amount_base=-base_amount,
                amount_quote=-quote_amount,
                amount_receipt=-pool_burns[0].amount
            )
            positions[position.content_id] = position

            liquidity = Liquidity(
                timestamp=tx.timestamp,
                tx_hash=tx.tx_hash,
                pool=log.contract,
                provider=provider,
                base_token=self.base_token,
                amount_base=-base_amount,
                quote_token=self.quote_token,
                amount_quote=-quote_amount,
                action="remove_lp",
                positions=positions,
                transfers=matched_transfers
            )
            
            result["events"][liquidity.content_id] = liquidity

            fee_collection_matched = msgspec.convert(fee_collection[0], type=MatchedTransfer) if fee_collection else None

            if fee_collection_matched:
                collection[fee_collection_matched.content_id] = fee_collection_matched
                matched_transfers[fee_collection_matched.content_id] = fee_collection_matched

                fee_received = TransferLedger(
                    timestamp=tx.timestamp,
                    tx_hash=tx.tx_hash,
                    token=log.contract,
                    address=fee_collection_matched.to_address.lower(),
                    amount=fee_collection_matched.amount,
                    action="received",
                    transfers=collection,
                    desc="Protocol fees collected",
                )

                result["events"][fee_received.content_id] = fee_received

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
            base_amount, quote_amount = self.get_in_out_amounts(log)
            if not self._validate_attr([base_amount, quote_amount], tx.tx_hash, log.index, result["errors"]):
                return result

            direction = self.get_direction(base_amount)
            unmatched_transfers = self._get_unmatched_transfers(tx)
            swap_transfers = self._get_swap_transfers(unmatched_transfers)
            

            base_swap = [t for t in swap_transfers["base_swaps"].values() if t.amount == abs(base_amount)]
            quote_swap = [t for t in swap_transfers["quote_swaps"].values() if t.amount == abs(quote_amount)]

            if len(base_swap) != 1:
                error = create_transform_error(
                    error_type="invalid_swap",
                    message=f"Expected exactly 1 base token swap, found {len(base_swap)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result
            
            if len(quote_swap) != 1:
                error = create_transform_error(
                    error_type="invalid_swap",
                    message=f"Expected exactly 1 quote token swap, found {len(quote_swap)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result
            
            base_matched = msgspec.convert(base_swap[0], type=MatchedTransfer)
            quote_matched = msgspec.convert(quote_swap[0], type=MatchedTransfer)
            matched_transfers = {
                base_matched.content_id: base_matched,
                quote_matched.content_id: quote_matched
            }

            taker = log.attributes.get("to").lower()
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
    
    def process_logs(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[Optional[Dict[str, Transfer]], Optional[Dict[str, DomainEvent]], Optional[Dict[str, ProcessingError]]]:
        """ Process logs and return matched transfers, events, and errors """
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
                
                except Exception as e:
                    self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, errors)

        except Exception as e:
            self._create_tx_exception(e, tx.tx_hash, self.__class__.__name__, errors)
        
        return (
            matched_transfers if matched_transfers else None, 
            new_events if new_events else None, 
            errors if errors else None
        )