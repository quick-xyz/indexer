# indexer/transform/manager.py

from typing import Tuple, Dict, List, Optional
import msgspec

from .registry import TransformerRegistry
from ..types import (
    Transaction, 
    DecodedLog,
    DomainEvent,
    Transfer,
    ProcessingError,
    DomainEventId,
    ErrorId,
    create_transform_error,
)
from ..core.mixins import LoggingMixin


class TransformationManager(LoggingMixin):   
    def __init__(self, registry: TransformerRegistry):
        self.registry = registry
        self.log_info("TransformationManager initialized")

    def process_transaction(self, transaction: Transaction) -> Tuple[bool, Transaction]:
        """Process a transaction through the transformation pipeline"""
        
        tx_context = self.log_transaction_context(transaction.tx_hash)
        
        if not self._has_decoded_logs(transaction) or not transaction.tx_success:
            self.log_debug("Skipping transaction - no decoded logs or failed transaction", **tx_context)
            return False, transaction

        self.log_info("Starting transaction processing", **tx_context)

        updated_tx = msgspec.convert(transaction, type=type(transaction))
        decoded_logs = self._get_decoded_logs(transaction)

        # Initialize collections
        if not updated_tx.transfers:
            updated_tx.transfers = {}
        if not updated_tx.signals:
            updated_tx.signals = {}
        if not updated_tx.events:
            updated_tx.events = {}
        if not updated_tx.errors:
            updated_tx.errors = {}

        # PHASE 1: PROCESS ALL LOGS THROUGH TRANSFORMERS
        self.log_debug("Starting transformer processing phase", 
                      decoded_log_count=len(decoded_logs), **tx_context)
        
        self._process_transformers(decoded_logs, updated_tx)
        
        # Log results from Phase 1
        signal_count = len(updated_tx.signals) if updated_tx.signals else 0
        direct_event_count = len(updated_tx.events) if updated_tx.events else 0
        transfer_count = len(updated_tx.transfers) if updated_tx.transfers else 0
        
        self.log_info("Transformer processing completed", 
                     signal_count=signal_count,
                     direct_event_count=direct_event_count,
                     transfer_count=transfer_count,
                     **tx_context)

        # PHASE 2: TRANSACTION MANAGER PROCESSES SIGNALS → ADDITIONAL EVENTS
        self.log_debug("Starting transaction manager phase", **tx_context)
        
        transaction_manager_events = self._process_transaction_level_events(updated_tx)
        
        # Add transaction manager events to the transaction
        if transaction_manager_events:
            updated_tx.events.update(transaction_manager_events)
            
        tm_event_count = len(transaction_manager_events) if transaction_manager_events else 0
        self.log_info("Transaction manager processing completed",
                     transaction_manager_events=tm_event_count,
                     **tx_context)
        
        # Final summary
        final_event_count = len(updated_tx.events) if updated_tx.events else 0
        error_count = len(updated_tx.errors) if updated_tx.errors else 0
        
        self.log_info("Transaction processing completed",
                     final_signal_count=signal_count,
                     final_event_count=final_event_count,
                     final_transfer_count=transfer_count,
                     final_error_count=error_count,
                     **tx_context)

        return True, updated_tx

    def _process_transformers(self, decoded_logs: Dict[int, DecodedLog], transaction: Transaction) -> None:
        """Process all decoded logs through transformers to create signals, direct events, and transfers"""
        
        # Group logs by contract
        logs_by_contract = {}
        for log_idx, log in decoded_logs.items():
            contract = log.contract.lower()
            if contract not in logs_by_contract:
                logs_by_contract[contract] = []
            logs_by_contract[contract].append(log)

        # Process each contract's logs
        for contract_address, contract_logs in logs_by_contract.items():
            transformer = self.registry.get_transformer(contract_address)
            
            if not transformer:
                self.log_debug("No transformer found for contract", 
                              contract_address=contract_address,
                              log_count=len(contract_logs),
                              tx_hash=transaction.tx_hash)
                continue
            
            transformer_context = {
                'contract_address': contract_address,
                'transformer_name': type(transformer).__name__,
                'tx_hash': transaction.tx_hash,
                'log_count': len(contract_logs)
            }
            
            self.log_debug("Processing contract logs", **transformer_context)
            
            try:
                # Call transformer's process_signals method
                # Expected signature: (signals, direct_events, transfers, errors)
                result = transformer.process_signals(contract_logs, transaction)
                
                # Handle different return tuple lengths for backward compatibility
                if len(result) == 3:
                    # Legacy format: (signals, transfers, errors)
                    signals, transfers, errors = result
                    direct_events = None
                elif len(result) == 4:
                    # New format: (signals, direct_events, transfers, errors)
                    signals, direct_events, transfers, errors = result
                else:
                    raise ValueError(f"Unexpected return tuple length: {len(result)}")
                
                # Add signals to transaction
                if signals:
                    transaction.signals.update(signals)
                    self.log_debug("Added signals", 
                                  signal_count=len(signals),
                                  **transformer_context)
                
                # Add direct events to transaction
                if direct_events:
                    transaction.events.update(direct_events)
                    self.log_debug("Added direct events", 
                                  event_count=len(direct_events),
                                  **transformer_context)
                
                # Add transfers to transaction
                if transfers:
                    transaction.transfers.update(transfers)
                    self.log_debug("Added transfers", 
                                  transfer_count=len(transfers),
                                  **transformer_context)
                
                # Add errors to transaction
                if errors:
                    transaction.errors.update(errors)
                    self.log_warning("Transformer errors", 
                                   error_count=len(errors),
                                   **transformer_context)
                
            except Exception as e:
                self.log_error("Exception in transformer processing",
                             error=str(e),
                             exception_type=type(e).__name__,
                             **transformer_context)
                
                error = create_transform_error(
                    error_type="transformer_processing_exception",
                    message=f"Exception in transformer processing: {str(e)}",
                    tx_hash=transaction.tx_hash,
                    contract_address=contract_address,
                    transformer_name=transformer.__class__.__name__
                )
                transaction.errors[error.error_id] = error

    def _process_transaction_level_events(self, transaction: Transaction) -> Optional[Dict[DomainEventId, DomainEvent]]:
        """
        Process signals and transfers to create transaction-level events.
        This is where complex aggregation logic will go (Trade events, arbitrage detection, etc.)
        """
        
        # Placeholder for transaction manager logic
        # This will handle:
        # - Grouping PoolSwapSignals into Trade events
        # - Arbitrage detection
        # - BLUB transfer reconciliation
        # - etc.
        
        tx_context = self.log_transaction_context(transaction.tx_hash)
        
        self.log_debug("Transaction manager processing (placeholder)", 
                      signal_count=len(transaction.signals) if transaction.signals else 0,
                      **tx_context)
        
        # For now, return empty - you'll implement the actual logic
        return {}

    def _get_decoded_logs(self, transaction: Transaction) -> Dict[int, DecodedLog]:
        """Get all decoded logs from transaction"""
        decoded_logs = {}
        for index, log in transaction.logs.items():
            if isinstance(log, DecodedLog):
                decoded_logs[index] = log
        return decoded_logs

    def _has_decoded_logs(self, transaction: Transaction) -> bool:
        """Check if transaction has any decoded logs"""
        return any(isinstance(log, DecodedLog) for log in transaction.logs.values())
    
    def get_processing_summary(self, transaction: Transaction) -> Dict[str, int]:
        """Get summary statistics for processed transaction"""
        return {
            "total_logs": len(transaction.logs),
            "decoded_logs": len(self._get_decoded_logs(transaction)),
            "signals": len(transaction.signals) if transaction.signals else 0,
            "transfers": len(transaction.transfers) if transaction.transfers else 0,
            "events": len(transaction.events) if transaction.events else 0,
            "errors": len(transaction.errors) if transaction.errors else 0
        }
    
    '''
    METHODS TO ADD TO TRANSFORMATION MANAGER FROM TRANSFORMERS
    '''
    def _get_unmatched_transfers(self, tx: Transaction) -> Dict[DomainEventId, Transfer]:
        """Get unmatched transfers with logging"""
        unmatched_transfers = {}
        
        if not tx.transfers:
            self.log_debug("No transfers found in transaction", 
                          tx_hash=tx.tx_hash,
                          transformer_name=self.name)
            return unmatched_transfers
    
        for transfer_id, transfer in tx.transfers.items():
            if isinstance(transfer, UnmatchedTransfer):
                unmatched_transfers[transfer_id] = transfer

        self.log_debug("Retrieved unmatched transfers",
                      tx_hash=tx.tx_hash,
                      total_transfers=len(tx.transfers),
                      unmatched_count=len(unmatched_transfers),
                      transformer_name=self.name)
        
        # Log details of unmatched transfers for debugging
        for transfer_id, transfer in unmatched_transfers.items():
            self.log_debug("Unmatched transfer detail",
                          transfer_id=transfer_id,
                          transfer_type=type(transfer).__name__,
                          token=transfer.token,
                          amount=transfer.amount,
                          from_address=transfer.from_address,
                          to_address=transfer.to_address,
                          involves_contract=(transfer.from_address == self.contract_address or 
                                           transfer.to_address == self.contract_address),
                          tx_hash=tx.tx_hash,
                          transformer_name=self.name)

        return unmatched_transfers

    def _get_all_transfers(self, tx: Transaction) -> Dict[DomainEventId, Transfer]:
        all_transfers = {}
        
        if not tx.transfers:
            return all_transfers
    
        for transfer_id, transfer in tx.transfers.items():
            if isinstance(transfer, Transfer):
                all_transfers[transfer_id] = transfer

        return all_transfers
    
    def _get_transfers_for_token(self, transfers: Dict[DomainEventId,Transfer], token: EvmAddress) -> Dict[DomainEventId,Transfer]:
        result = {}
        for transfer_id, transfer in transfers.items():
            if transfer.token == token.lower():
                result[transfer_id]= transfer
        return result
    
    def _get_decoded_logs(self, tx: Transaction) -> Dict[int, DecodedLog]:
        decoded_logs = {}
        for index, log in tx.logs.items():
            if isinstance(log, DecodedLog):
                decoded_logs[index] = log
        return decoded_logs

    def _has_decoded_logs(transaction: Transaction) -> bool:
        return any(isinstance(log, DecodedLog) for log in transaction.logs.values())

    def _build_transfer_from_log(self, log: DecodedLog, transaction: Transaction) -> Optional[Transfer]:
        """Build transfer from log with logging"""
        try:
            from_addr = EvmAddress(str(log.attributes.get("from", "")).lower())
            to_addr = EvmAddress(str(log.attributes.get("to", "")).lower())
            value = amount_to_str(log.attributes.get("value", 0))
            
            transfer_context = {
                'log_index': log.index,
                'event_name': log.name,
                'contract': log.contract,
                'from_address': from_addr,
                'to_address': to_addr,
                'amount': value,
                'tx_hash': transaction.tx_hash,
                'transformer_name': self.name
            }
            
            if not from_addr or not to_addr or is_zero(value):
                self.log_debug("Transfer creation skipped - invalid attributes", **transfer_context)
                return None
                
            transfer = UnmatchedTransfer(
                timestamp=transaction.timestamp,
                tx_hash=transaction.tx_hash,
                log_index=log.index,
                token=log.contract.lower(),
                amount=value,
                from_address=from_addr,
                to_address=to_addr
            )
            
            self.log_debug("Transfer created from log", 
                          transfer_id=transfer.content_id,
                          **transfer_context)
            
            return transfer
            
        except Exception as e:
            self.log_error("Exception in transfer creation",
                          error=str(e),
                          exception_type=type(e).__name__,
                          log_index=log.index,
                          tx_hash=transaction.tx_hash,
                          transformer_name=self.name)
            return None

    def _convert_to_matched_transfer(self, transfer: Transfer) -> MatchedTransfer:
        """Convert transfer to matched with logging"""
        self.log_debug("Converting transfer to matched",
                      transfer_type=type(transfer).__name__,
                      transfer_id=getattr(transfer, 'content_id', 'unknown'),
                      token=transfer.token,
                      amount=transfer.amount,
                      transformer_name=self.name)
        
        return MatchedTransfer(
            timestamp=transfer.timestamp,
            tx_hash=transfer.tx_hash,
            log_index=transfer.log_index,
            content_id=transfer.content_id,
            token=transfer.token,
            amount=transfer.amount,
            from_address=transfer.from_address,
            to_address=transfer.to_address,
            transfer_type=getattr(transfer, 'transfer_type', 'transfer'),
            batch=getattr(transfer, 'batch', None)
        )

    def _create_matched_transfers_dict(self, transfers: List[Transfer]) -> Dict[DomainEventId, MatchedTransfer]:
        """Create matched transfers dict with logging"""
        matched_transfers = {}
        
        self.log_debug("Creating matched transfers dict",
                      input_count=len(transfers),
                      transformer_name=self.name)
        
        for transfer in transfers:
            matched = self._convert_to_matched_transfer(transfer)
            matched_transfers[matched.content_id] = matched
            
            self.log_debug("Transfer converted to matched",
                          original_type=type(transfer).__name__,
                          matched_id=matched.content_id,
                          token=matched.token,
                          amount=matched.amount,
                          transformer_name=self.name)
        
        self.log_debug("Matched transfers dict created",
                      output_count=len(matched_transfers),
                      transformer_name=self.name)
                      
        return matched_transfers
    
    def _calculate_net_amounts_by_token(self, transfers: List[Transfer], 
                                      contract_address: EvmAddress) -> Dict[EvmAddress, str]:
        """
        Calculate net amounts by token from a list of transfers.
        
        Positive = net inflow to contract
        Negative = net outflow from contract
        """
        net_amounts = {}
        
        for transfer in transfers:
            token = transfer.token
            if token not in net_amounts:
                net_amounts[token] = "0"
                
            if transfer.to_address == contract_address.lower():
                net_amounts[token] = amount_to_str(amount_to_int(net_amounts[token]) + amount_to_int(transfer.amount))
            elif transfer.from_address == contract_address.lower():
                net_amounts[token] = amount_to_str(amount_to_int(net_amounts[token]) - amount_to_int(transfer.amount))
                
        return net_amounts
    
    def _validate_transfer_count(self, transfers: List[Transfer], name: str, expected_count: int,
                                tx_hash: EvmHash, log_index: int, error_type: str,
                                error_dict: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate transfer count with detailed logging"""
        if len(transfers) != expected_count:
            self.log_error("Transfer count validation failed",
                          transfer_type=name,
                          expected_count=expected_count,
                          actual_count=len(transfers),
                          tx_hash=tx_hash,
                          log_index=log_index,
                          transformer_name=self.name)
            
            # Log details of found transfers for debugging
            for i, transfer in enumerate(transfers):
                self.log_debug("Transfer candidate detail",
                              candidate_index=i,
                              transfer_id=getattr(transfer, 'content_id', 'unknown'),
                              token=transfer.token,
                              amount=transfer.amount,
                              from_address=transfer.from_address,
                              to_address=transfer.to_address,
                              tx_hash=tx_hash,
                              transformer_name=self.name)
            
            error = create_transform_error(
                error_type=error_type,
                message=f"Expected exactly {expected_count} {name} transfers, found {len(transfers)}",
                tx_hash=tx_hash,
                log_index=log_index
            )
            error_dict[error.error_id] = error
            return False
            
        self.log_debug("Transfer count validation passed",
                      transfer_type=name,
                      count=expected_count,
                      tx_hash=tx_hash,
                      log_index=log_index,
                      transformer_name=self.name)
        return True
    
    def _is_router_mediated(self, transaction: Transaction, provider: EvmAddress) -> bool:
        """
        Check if operation is router-mediated by comparing provider to transaction origin.
        
        Router-mediated: provider != tx.origin_from
        Direct: provider == tx.origin_from
        """
        return provider.lower() != transaction.origin_from.lower()

    def _extract_provider_from_transfers(self, transfers: List[Transfer], 
                                       operation: str = "mint") -> Optional[EvmAddress]:
        """
        Extract the actual liquidity provider from transfer patterns.
        
        For mints: provider is the address sending tokens TO the pool
        For burns: provider is the address receiving tokens FROM the pool
        """
        if not transfers:
            return None
            
        if operation == "mint":
            for transfer in transfers:
                if transfer.to_address == self.contract_address:
                    return transfer.from_address
        elif operation == "burn":
            for transfer in transfers:
                if transfer.from_address == self.contract_address:
                    return transfer.to_address
                    
        return None

    def _sum_transfer_amounts(self, transfers: List[Transfer]) -> str:
        total = 0
        for t in transfers:
            total += amount_to_int(t.amount)
        return amount_to_str(total)

    def _filter_transfers_by_direction(self, transfers: List[Transfer], 
                                     to_contract: bool = True) -> List[Transfer]:
        """
        Filter transfers by direction relative to contract.
        
        Args:
            to_contract: True for transfers TO contract, False for transfers FROM contract
        """
        if not self.contract_address:
            return []
            
        if to_contract:
            return [t for t in transfers if t.to_address == self.contract_address]
        else:
            return [t for t in transfers if t.from_address == self.contract_address]

    def _find_transfers_by_criteria(self, transfers: List[Transfer], token: EvmAddress,
                                  amount: str, from_addr: Optional[EvmAddress] = None,
                                  to_addr: Optional[EvmAddress] = None) -> List[Transfer]:
        """Find transfers matching specific criteria with logging"""
        filtered = [t for t in transfers if t.token == token.lower() and compare_amounts(t.amount, amount) == 0]
        
        if from_addr:
            filtered = [t for t in filtered if t.from_address == from_addr.lower()]
        if to_addr:
            filtered = [t for t in filtered if t.to_address == to_addr.lower()]
        
        self.log_debug("Transfer criteria search",
                      input_count=len(transfers),
                      token=token,
                      amount=amount,
                      from_addr=from_addr,
                      to_addr=to_addr,
                      matches_found=len(filtered),
                      transformer_name=self.name)
            
        return filtered
    
    ''' This one should be similar to log_exception in BaseTransformer '''
    def _create_tx_exception(self, e: Exception, tx_hash: EvmHash, transformer_name: str, 
                            error_dict: Dict[ErrorId, ProcessingError]) -> None:
        self.log_error("Transaction processing exception",
                      error=str(e),
                      exception_type=type(e).__name__,
                      tx_hash=tx_hash,
                      transformer_name=transformer_name)
        
        error = create_transform_error(
            error_type="processing_exception",
            message=f"Transaction processing exception: {str(e)}",
            tx_hash=tx_hash,
            transformer_name=transformer_name
        )
        error_dict[error.error_id] = error

    ''' From applied transformers'''
    def _get_liquidity_transfers(self, unmatched_transfers: Dict[DomainEventId, Transfer]) -> Dict[str, Dict[DomainEventId, Transfer]]:
        liq_transfers = {
            "mints": {},
            "burns": {},
            "deposits": {},
            "withdrawals": {},
            "underlying_transfers": {},
            "receipt_transfers": {}
        }

        for key, transfer in unmatched_transfers.items():
            if transfer.token == self.contract_address:
                if transfer.from_address == ZERO_ADDRESS:
                    liq_transfers["mints"][key] = transfer
                elif transfer.to_address == ZERO_ADDRESS:
                    liq_transfers["burns"][key] = transfer
                else:
                    liq_transfers["receipt_transfers"][key] = transfer

            elif transfer.token in [self.base_token, self.quote_token]:
                if transfer.to_address == self.contract_address:
                    liq_transfers["deposits"][key] = transfer
                elif transfer.from_address == self.contract_address:
                    liq_transfers["withdrawals"][key] = transfer
                else:
                    liq_transfers["underlying_transfers"][key] = transfer
                        
        return liq_transfers

    def _get_swap_transfers(self, unmatched_transfers: Dict[DomainEventId, Transfer]) -> Dict[str, Dict[DomainEventId, Transfer]]:
        swap_transfers = {"base_swaps": {},"quote_swaps": {}}

        for key, transfer in unmatched_transfers.items():
            if not (transfer.from_address == self.contract_address or transfer.to_address == self.contract_address):
                continue
            
            if transfer.token == self.base_token:
                swap_transfers["base_swaps"][key] = transfer
            elif transfer.token == self.quote_token:
                swap_transfers["quote_swaps"][key] = transfer

        return swap_transfers

    def _get_swap_direction(self, base_amount: str) -> str:
        return "buy" if is_positive(base_amount) else "sell"
    
    def _validate_transfer_counts_flexible(self, transfer_groups: List[Tuple], tx_hash: EvmHash, log_index: int, 
                                         error_type: str, error_dict: Dict[ErrorId, ProcessingError]) -> bool:
        for transfers, name, expected, is_max in transfer_groups:
            if is_max:
                if len(transfers) > expected:
                    error = create_transform_error(
                        error_type=error_type,
                        message=f"Expected at most {expected} {name}, found {len(transfers)}",
                        tx_hash=tx_hash, log_index=log_index
                    )
                    error_dict[error.error_id] = error
                    return False
            else:
                if not self._validate_transfer_count(transfers, name, expected, tx_hash, log_index, error_type, error_dict):
                    return False
        return True
    
    def _handle_batch_transfers(self, transfers_to_match: List[Transfer]) -> Dict[DomainEventId, MatchedTransfer]:
        return self._create_matched_transfers_dict(transfers_to_match)
    
    def process_transfers(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[Optional[Dict[DomainEventId,Transfer]],Optional[Dict[ErrorId,ProcessingError]]]:
        transfers, errors = {}, {}

        self.log_info("Starting transfer processing", 
                     tx_hash=tx.tx_hash,
                     log_count=len(logs))

        for log in logs:
            try:
                if log.name == "Transfer":
                    transfer = self._build_transfer_from_log(log, tx)
                    if transfer:
                        transfers[transfer.content_id] = transfer
            except Exception as e:
                self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, errors)
        
        self.log_info("Transfer processing completed", 
                     tx_hash=tx.tx_hash,
                     transfers_created=len(transfers),
                     errors_created=len(errors))
                
        return transfers if transfers else None, errors if errors else None