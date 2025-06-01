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
from ....utils.lb_byte32_decoder import decode_amounts


class LbPairTransformer(BaseTransformer):
    def __init__(self, contract: EvmAddress, token_x: EvmAddress, token_y: EvmAddress, base_token: EvmAddress):
        super().__init__(contract_address=contract.lower())
        self.token_x = token_x.lower()
        self.token_y = token_y.lower()
        self.base_token = base_token.lower()
        self.quote_token = self.token_y if self.token_x == self.base_token else self.token_x

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
    
    def _unpack_amounts(self, bytes: bytes) -> tuple[Optional[int], Optional[int]]:
        try:
            amounts_x, amounts_y = decode_amounts(bytes)

            if self.token_x == self.base_token:
                return amounts_x, amounts_y
            else:
                return amounts_y, amounts_x
        except Exception:
            return None, None

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
            provider = log.attributes.get("to").lower()
            amounts = log.attributes.get("amounts")
            bins = log.attributes.get("ids")

            if len(amounts) != len(bins):
                error = create_transform_error(
                    error_type= "invalid_lb_transfer",
                    message = f"LB Mint: Expected amounts and bins to have the same length, got {len(amounts)} and {len(bins)}",
                    tx_hash = tx.tx_hash,
                    log_index = log.index
                )
                result["errors"][error.error_id] = error
                return result

            if not self._validate_attr([provider, amounts, bins], tx.tx_hash, log.index, result["errors"]):
                return result

            unmatched_transfers = self._get_unmatched_transfers(tx)
            liq_transfers = self._get_liquidity_transfers(unmatched_transfers)

            base_deposits = [t for t in liq_transfers["deposits"].values() if t.token == self.base_token]
            quote_deposits = [t for t in liq_transfers["deposits"].values() if t.token == self.quote_token]
            pool_mints = [t for t in liq_transfers["mints"].values()]
            base_refunds = [t for t in liq_transfers["withdrawals"].values() 
                        if t.token == self.base_token and t.to_address == provider]
            quote_refunds = [t for t in liq_transfers["withdrawals"].values() 
                            if t.token == self.quote_token and t.to_address == provider]

            if len(base_deposits) > 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_deposit",
                    message=f"Expected at most 1 base token deposit, found {len(base_deposits)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                if not error.context:
                    error.context = {}
                error.context['log_index'] = log.index
                result["errors"][error.error_id] = error
                return result
                
            if len(quote_deposits) > 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_deposit", 
                    message=f"Expected at most 1 quote token deposit, found {len(quote_deposits)}",
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
            
            if len(base_refunds) > 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_deposit",
                    message=f"Expected at most 1 base token refund, found {len(base_refunds)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result
            
            if len(quote_refunds) > 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_deposit", 
                    message=f"Expected at most 1 quote token refund, found {len(quote_refunds)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result

            mint_amounts = pool_mints[0].batch
            mint_bins = list(mint_amounts.keys())

            if mint_bins != bins:
                error = create_transform_error(
                    error_type="invalid_liquidity_deposit",
                    message="Mint transfer bins do not match deposit event bins",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result

            positions = {}
            sum_base, sum_quote, sum_receipt = 0, 0, 0

            for i in bins:
                base_amount, quote_amount = self._unpack_amounts(amounts[i])
                receipt_amount = mint_amounts[i]

                bin_liquidity = Position(
                    timestamp=tx.timestamp,
                    tx_hash=tx.tx_hash,
                    receipt_token=log.contract,
                    receipt_id=i,
                    amount_base=base_amount,
                    amount_quote=quote_amount,
                    amount_receipt=receipt_amount,
                )
                positions[bin_liquidity.content_id] = bin_liquidity
                sum_base += base_amount
                sum_quote += quote_amount
                sum_receipt += receipt_amount
            
            net_base = (base_deposits[0].amount if base_deposits else 0) - (base_refunds[0].amount if base_refunds else 0)
            net_quote = (quote_deposits[0].amount if quote_deposits else 0) - (quote_refunds[0].amount if quote_refunds else 0)

            if not (
                sum_base == net_base and
                sum_quote == net_quote and
                sum_receipt == pool_mints[0].amount
            ):
                error = create_transform_error(
                    error_type="invalid_liquidity_deposit",
                    message="Sum of amounts in positions does not match net deposit transfer amounts",
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
            if base_refunds:
                base_refund_matched = msgspec.convert(base_refunds[0], type=MatchedTransfer)
                matched_transfers[base_refund_matched.content_id] = base_refund_matched
            if quote_refunds:
                quote_refund_matched = msgspec.convert(quote_refunds[0], type=MatchedTransfer)
                matched_transfers[quote_refund_matched.content_id] = quote_refund_matched        

            pool_matched = msgspec.convert(pool_mints[0], type=MatchedTransfer)
            matched_transfers[pool_matched.content_id] = pool_matched

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
        result = {
            "transfers": {},
            "events": {},
            "errors": {}
        }
        
        try:
            amounts = log.attributes.get("amounts")
            bins = log.attributes.get("ids")

            if len(amounts) != len(bins):
                error = create_transform_error(
                    error_type="invalid_lb_transfer",
                    message=f"LB Burn: Expected amounts and bins to have the same length, got {len(amounts)} and {len(bins)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result

            if not self._validate_attr([amounts, bins], tx.tx_hash, log.index, result["errors"]):
                return result

            unmatched_transfers = self._get_unmatched_transfers(tx)
            liq_transfers = self._get_liquidity_transfers(unmatched_transfers)

            base_withdrawals = [t for t in liq_transfers["withdrawals"].values() if t.token == self.base_token]
            quote_withdrawals = [t for t in liq_transfers["withdrawals"].values() if t.token == self.quote_token]
            pool_burns = [t for t in liq_transfers["burns"].values()]

            if len(base_withdrawals) > 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_withdrawal",
                    message=f"Expected at most 1 base token withdrawal, found {len(base_withdrawals)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result
                
            if len(quote_withdrawals) > 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_withdrawal", 
                    message=f"Expected at most 1 quote token withdrawal, found {len(quote_withdrawals)}",
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
            
            router = log.attributes.get("to").lower()
            provider = pool_burns[0].from_address.lower()
            base_router_transfers, quote_router_transfers = [], []
            
            if provider != router:
                base_router_transfers = [t for t in liq_transfers["underlying_transfers"].values() 
                                    if t.token == self.base_token and t.from_address == router and t.to_address == provider and t.amount == base_withdrawals[0].amount]
                quote_router_transfers = [t for t in liq_transfers["underlying_transfers"].values() 
                                        if t.token == self.quote_token and t.from_address == router and t.to_address == provider and t.amount == quote_withdrawals[0].amount]
            
            if len(base_router_transfers) > 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_withdrawal",
                    message=f"Expected at most 1 base token final transfer, found {len(base_router_transfers)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result

            if len(quote_router_transfers) > 1:
                error = create_transform_error(
                    error_type="invalid_liquidity_withdrawal",
                    message=f"Expected at most 1 base token final transfer, found {len(quote_router_transfers)}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result
            
            burn_amounts = pool_burns[0].batch
            burn_bins = list(burn_amounts.keys())

            if burn_bins != bins:
                error = create_transform_error(
                    error_type="invalid_liquidity_withdrawal",
                    message="Burn transfer bins do not match withdrawal event bins",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error
                return result

            positions = {}
            sum_base, sum_quote, sum_receipt = 0, 0, 0

            for i in bins:
                base_amount, quote_amount = self._unpack_amounts(amounts[i])
                receipt_amount = burn_amounts[i]

                bin_liquidity = Position(
                    timestamp=tx.timestamp,
                    tx_hash=tx.tx_hash,
                    receipt_token=log.contract,
                    receipt_id=i,
                    amount_base=-base_amount,
                    amount_quote=-quote_amount,
                    amount_receipt=-receipt_amount,
                )
                positions[bin_liquidity.content_id] = bin_liquidity
                sum_base += base_amount
                sum_quote += quote_amount
                sum_receipt += receipt_amount
            
            if not (
                sum_base == (base_withdrawals[0].amount if base_withdrawals else 0) and
                sum_quote == (quote_withdrawals[0].amount if quote_withdrawals else 0) and
                sum_receipt == pool_burns[0].amount
            ):
                error = create_transform_error(
                    error_type="invalid_liquidity_withdrawal",
                    message="Sum of amounts in positions does not match withdrawal transfer amounts",
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
            if provider != router:
                if base_router_transfers:
                    base_router_matched = msgspec.convert(base_router_transfers[0], type=MatchedTransfer)
                    matched_transfers[base_router_matched.content_id] = base_router_matched
                if quote_router_transfers:
                    quote_router_matched = msgspec.convert(quote_router_transfers[0], type=MatchedTransfer)
                    matched_transfers[quote_router_matched.content_id] = quote_router_matched

            pool_matched = msgspec.convert(pool_burns[0], type=MatchedTransfer)
            matched_transfers[pool_matched.content_id] = pool_matched

            liquidity = Liquidity(
                timestamp=tx.timestamp,
                tx_hash=tx.tx_hash,
                pool=log.contract,
                provider=provider,
                base_token=self.base_token,
                amount_base=-sum_base,
                quote_token=self.quote_token,
                amount_quote=-sum_quote,
                action="remove_lp",
                positions=positions,
                transfers=matched_transfers
            )

            result["events"][liquidity.content_id] = liquidity
            result["transfers"] = matched_transfers

        except Exception as e:
            self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, result["errors"])

        return result
    
    def process_transfers(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[Optional[Dict[DomainEventId,Transfer]],Optional[Dict[ErrorId,ProcessingError]]]:
        transfers = {}
        errors = {}

        for log in logs:
            try:
                if log.name == "TransferBatch":
                    from_addr = log.attributes.get("from").lower()
                    to_addr = log.attributes.get("to").lower()
                    amounts = log.attributes.get("amounts")
                    bins = log.attributes.get("ids")
                    
                    if not len(amounts) == len(bins):
                        error = create_transform_error(
                            error_type= "invalid_lb_transfer",
                            message = f"LB Transfer Batch: Expected amounts and bins to have the same length, got {len(amounts)} and {len(bins)}",
                            tx_hash = tx.tx_hash,
                            log_index = log.index
                        )
                        errors[error.id] = error
                        continue
                    if not self._validate_attr([from_addr, to_addr, amounts, bins], tx.tx_hash, log.index, errors):
                        continue

                    transferids = {}
                    sum_transfers = 0
                    
                    for i in bins:    
                        transferids[i] = amounts[i]
                        sum_transfers += amounts[i]

                    transfer = UnmatchedTransfer(
                        timestamp=tx.timestamp,
                        tx_hash=tx.tx_hash,
                        token=log.contract,
                        amount=sum_transfers,
                        from_address=from_addr,
                        to_address=to_addr,
                        transfer_type="transfer_batch",
                        batch=transferids,
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

                    elif log.name == "DepositedToBins":
                        mint_result = self._handle_mint(log, tx)
                        if mint_result:
                            new_events.update(mint_result["events"])
                            matched_transfers.update(mint_result["transfers"])
                            errors.update(mint_result["errors"])

                    elif log.name == "WithdrawnFromBins":
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