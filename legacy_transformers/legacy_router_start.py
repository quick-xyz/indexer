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

    def _get_router_transfers(self, unmatched_transfers: Dict[EvmAddress, Dict[DomainEventId, Transfer]]) -> Dict[str, Dict[DomainEventId, Transfer]]:
        """Get transfers related to the router contract"""
        router_transfers = {
            "token_in": {},
            "token_out": {},
        }

        for contract, trf_dict in unmatched_transfers.items():
            for key, transfer in trf_dict.items():
                # Transfers TO the router (user sending tokens in)
                if transfer.to_address == self.contract_address:
                    router_transfers["token_in"][key] = transfer
                # Transfers FROM the router (user receiving tokens out)
                elif transfer.from_address == self.contract_address:
                    router_transfers["token_out"][key] = transfer

        return router_transfers

    def _collect_constituent_events(self, tx: Transaction, token_in: EvmAddress, token_out: EvmAddress) -> Dict[DomainEventId, Union[PoolSwap, Trade]]:
        """
        Collect constituent events based on aggregation mode.
        
        Router mode: Collects PoolSwap events only
        Aggregator mode: Collects both PoolSwap and Trade events
        """
        constituent_events = {}
        
        if not tx.events:
            return constituent_events
            
        for event_id, event in tx.events.items():
            # Always collect PoolSwap events
            if isinstance(event, PoolSwap):
                if self._event_involves_tokens(event, token_in, token_out):
                    constituent_events[event_id] = event
            
            # In aggregator mode, also collect Trade events from other routers
            elif self.aggregation_mode == "aggregator" and isinstance(event, Trade):
                if self._event_involves_tokens(event, token_in, token_out):
                    # Don't aggregate trades from the same router (avoid recursion)
                    if event.router != self.contract_address:
                        constituent_events[event_id] = event
                    
        return constituent_events

    def _event_involves_tokens(self, event: Union[PoolSwap, Trade], token_in: EvmAddress, token_out: EvmAddress) -> bool:
        """Check if an event involves the specified input/output tokens"""
        return (event.base_token == token_in.lower() or event.quote_token == token_in.lower() or
                event.base_token == token_out.lower() or event.quote_token == token_out.lower())

    def _calculate_net_amounts_from_events(self, constituent_events: Dict[DomainEventId, Union[PoolSwap, Trade]], 
                                         token_in: EvmAddress, token_out: EvmAddress) -> Tuple[int, int]:
        """
        Calculate net amounts from constituent events (PoolSwaps and/or Trades).
        
        Returns (net_token_in_amount, net_token_out_amount)
        """
        net_token_in = 0
        net_token_out = 0
        
        for event in constituent_events.values():
            # Calculate net effect on our target tokens
            if event.base_token == token_in.lower():
                if event.direction == "sell":
                    net_token_in += event.base_amount
                else:
                    net_token_in -= event.base_amount
            elif event.quote_token == token_in.lower():
                if event.direction == "buy":
                    net_token_in += event.quote_amount
                else:
                    net_token_in -= event.quote_amount
                    
            if event.base_token == token_out.lower():
                if event.direction == "buy":
                    net_token_out += event.base_amount
                else:
                    net_token_out -= event.base_amount
            elif event.quote_token == token_out.lower():
                if event.direction == "sell":
                    net_token_out += event.quote_amount
                else:
                    net_token_out -= event.quote_amount
                    
        return abs(net_token_in), abs(net_token_out)

    def _create_reconciling_swap(self, tx: Transaction, token_in: EvmAddress, token_out: EvmAddress,
                               expected_in: int, expected_out: int, actual_in: int, actual_out: int,
                               taker: EvmAddress, log_index: int) -> Optional[Swap]:
        """Create a reconciling Swap event to account for differences between expected and actual amounts"""
        diff_in = expected_in - actual_in
        diff_out = expected_out - actual_out
        
        # Only create reconciling swap if there's a meaningful difference
        if abs(diff_in) < 100 and abs(diff_out) < 100:  # Allow for small rounding differences
            return None
            
        # Determine base/quote and direction for reconciling swap
        base_token, base_amount, quote_token, quote_amount, direction = self._determine_base_quote_from_amounts(
            token_in, token_out, abs(diff_in), abs(diff_out)
        )
        
        return Swap(
            timestamp=tx.timestamp,
            tx_hash=tx.tx_hash,
            taker=taker,
            direction=direction,
            base_token=base_token,
            base_amount=base_amount,
            quote_token=quote_token,
            quote_amount=quote_amount,
            log_index=log_index
        )

    def _determine_base_quote_from_amounts(self, token_in: EvmAddress, token_out: EvmAddress, 
                                         amount_in: int, amount_out: int) -> Tuple[EvmAddress, int, EvmAddress, int, str]:
        """Determine base/quote tokens and direction from amounts"""
        # Simple heuristic: WAVAX is usually quote token
        if token_in.lower() == self.wnative:
            # Selling AVAX for other token = sell direction
            return token_out, amount_out, token_in, amount_in, "sell"
        elif token_out.lower() == self.wnative:
            # Buying AVAX with other token = buy direction  
            return token_out, amount_out, token_in, amount_in, "buy"
        else:
            # For non-AVAX pairs, treat token_out as base (what user is buying)
            return token_out, amount_out, token_in, amount_in, "buy"

    def _handle_swap_event(self, log: DecodedLog, tx: Transaction, swap_event_name: str) -> Dict[str, Dict]:
        """
        Generic handler for swap events that can be customized by subclasses.
        
        Args:
            log: The swap event log
            tx: Transaction context
            swap_event_name: Name of the swap event for error reporting
        """
        result = {
            "transfers": {},
            "events": {},
            "errors": {}
        }
        
        try:
            # Extract common swap attributes - subclasses can override this
            swap_data = self._extract_swap_data(log)
            if not swap_data:
                return result
                
            sender = swap_data["sender"]
            to = swap_data["to"]
            token_in = swap_data["token_in"]
            token_out = swap_data["token_out"]
            amount_in = swap_data["amount_in"]
            amount_out = swap_data["amount_out"]
            
            if not self._validate_attr([sender, to, token_in, token_out, amount_in, amount_out], 
                                     tx.tx_hash, log.index, result["errors"]):
                return result

            unmatched_transfers = self._get_unmatched_transfers(tx)
            router_transfers = self._get_router_transfers(unmatched_transfers)

            # Find net transfers for this router operation
            input_transfers = [t for t in router_transfers["token_in"].values() 
                             if t.token == token_in.lower() and t.from_address == sender.lower()]
            output_transfers = [t for t in router_transfers["token_out"].values() 
                              if t.token == token_out.lower() and t.to_address == to.lower()]

            # Sum up all transfers (may be multiple for complex routing)
            total_input = sum(t.amount for t in input_transfers)
            total_output = sum(t.amount for t in output_transfers)

            # Validate transfers match event amounts
            if total_input != amount_in:
                error = create_transform_error(
                    error_type="router_amount_mismatch",
                    message=f"Input transfer sum {total_input} != event amount {amount_in}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error

            if total_output != amount_out:
                error = create_transform_error(
                    error_type="router_amount_mismatch",
                    message=f"Output transfer sum {total_output} != event amount {amount_out}",
                    tx_hash=tx.tx_hash,
                    log_index=log.index
                )
                result["errors"][error.error_id] = error

            # Collect constituent events based on aggregation mode
            constituent_events = self._collect_constituent_events(tx, token_in, token_out)
            
            # Calculate net amounts from constituent events
            net_token_in, net_token_out = self._calculate_net_amounts_from_events(constituent_events, token_in, token_out)

            # Create matched transfers
            matched_transfers = {}
            for transfer in input_transfers + output_transfers:
                matched = msgspec.convert(transfer, type=MatchedTransfer)
                matched_transfers[matched.content_id] = matched

            # Check if constituent events sum to router total
            all_swaps = dict(constituent_events)  # Copy constituent events
            
            if net_token_in != amount_in or net_token_out != amount_out:
                # Create reconciling swap for the difference
                reconciling_swap = self._create_reconciling_swap(
                    tx, token_in, token_out, amount_in, amount_out, 
                    net_token_in, net_token_out, sender.lower(), log.index
                )
                
                if reconciling_swap:
                    all_swaps[reconciling_swap.content_id] = reconciling_swap
                    
                    # Create error for unknown swap
                    error_type = f"unknown_{self.aggregation_mode}_swap_detected"
                    error = create_transform_error(
                        error_type=error_type,
                        message=f"Created reconciling swap for difference in {self.aggregation_mode} mode: expected_in={amount_in}, actual_in={net_token_in}, expected_out={amount_out}, actual_out={net_token_out}",
                        tx_hash=tx.tx_hash,
                        log_index=log.index
                    )
                    result["errors"][error.error_id] = error

            # Determine base/quote and direction for trade
            base_token, base_amount, quote_token, quote_amount, direction = self._determine_base_quote_from_amounts(
                token_in, token_out, amount_in, amount_out
            )

            # Create comprehensive Trade event
            trade = Trade(
                timestamp=tx.timestamp,
                tx_hash=tx.tx_hash,
                taker=sender.lower(),
                direction=direction,
                base_token=base_token,
                base_amount=base_amount,
                quote_token=quote_token,
                quote_amount=quote_amount,
                trade_type=self.trade_type,
                router=self.contract_address,
                swaps=all_swaps,
                log_index=log.index
            )

            result["events"][trade.content_id] = trade
            result["transfers"] = matched_transfers

        except Exception as e:
            self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, result["errors"])

        return result

    def _extract_swap_data(self, log: DecodedLog) -> Optional[Dict[str, Any]]:
        """
        Extract swap data from log. Can be overridden by subclasses for different event formats.
        
        Default implementation handles LFJ Aggregator format.
        """
        try:
            return {
                "sender": log.attributes.get("sender"),
                "to": log.attributes.get("to"),
                "token_in": log.attributes.get("tokenIn"),
                "token_out": log.attributes.get("tokenOut"),
                "amount_in": log.attributes.get("amountIn"),
                "amount_out": log.attributes.get("amountOut")
            }
        except Exception:
            return None

    def process_transfers(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[Optional[Dict[DomainEventId, Transfer]], Optional[Dict[ErrorId, ProcessingError]]]:
        """Process Transfer events"""
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
        """
        Process logs and return matched transfers, events, and errors.
        
        This method should be overridden by subclasses to handle specific event types.
        """
        raise NotImplementedError("Subclasses must implement process_logs method")