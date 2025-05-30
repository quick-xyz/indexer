from typing import List, Dict, Tuple, Optional, Literal

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
)


class LfjPoolTransformer(BaseTransformer):
    def __init__(self, contract: EvmAddress, token0: EvmAddress, token1: EvmAddress, base_token: EvmAddress, fee_collector: EvmAddress):
        super().__init__(contract_address=contract.lower())
        self.token0 = token0.lower()
        self.token1 = token1.lower()
        self.base_token = base_token.lower()
        self.quote_token = self.token1 if self.token0 == self.base_token else self.token0
        self.fee_collector = fee_collector.lower()

    
    def get_amounts(self, log: DecodedLog) -> tuple[int, int]:
        amount0 = log.attributes.get("amount0")
        amount1 = log.attributes.get("amount1")

        if self.token0 == self.base_token:
            return amount0, amount1
        else:
            return amount1, amount0


    def get_direction(self, base_amount: int) -> str:
        return "buy" if base_amount > 0 else "sell"
    

    def process_transfers(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[Optional[List[Transfer]],Optional[List[ProcessingError]]]:
        transfers = []
        errors = []

        for log in logs:
            try:
                if log.name == "Transfer":
                    from_addr = log.attributes.get("from")
                    to_addr = log.attributes.get("to")
                    value = log.attributes.get("value")
                    
                    if not all([from_addr, to_addr, value is not None]):
                        errors.append(ProcessingError(
                            stage="process_transfers",
                            error="missing_attributes",
                            desc=f"Transfer log missing attributes: {log.index}",
                        ))
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
                    key = transfer.generate_content_id()
                    transfers.append(transfer)

            except Exception as e:
                errors.append(ProcessingError(
                    stage="process_transfers",
                    error="processing_exception",
                    desc=f"Failed to process transfer log {log.index}: {str(e)}",
                ))
                
        return transfers if transfers else None, errors if errors else None

    def get_liquidity_transfers(self, unmatched_transfers: Dict[str,Dict[str, Transfer]]) -> Dict[str, Dict[str,Transfer]]:
        liq_transfers = {
            "mints": {},
            "burns": {},
            "deposits": {},
            "withdrawals": {},
            "receipt_transfers": {},
            "underlying_transfers": {}
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
    
    def handle_mint(self, log: DecodedLog, context: TransactionContext) -> list[DomainEvent]:
        base_amount, quote_amount = self.get_amounts(log)

        remaining_transfers = context.transaction.transfers.unmatched.copy()
        liq_transfers = self
    
    def process_logs(self, logs: List[DecodedLog], events: Dict[str,DomainEvent], tx: Transaction) -> Tuple[Dict[str,Transfer],Dict[str,DomainEvent],List[ProcessingError]]:

        unmatched_transfers = self.get_unmatched_transfers(tx)

        for log in logs:
            if log.name == "Swap":
                #TODO
                continue

            elif log.name == "Mint":
                base_amount, quote_amount = self.get_amounts(log)
                liq_transfers = self.get_liquidity_transfers(unmatched_transfers)

                if liq_transfers["deposits"]:
                    for key, transfer in liq_transfers["deposits"].items():
                        contract = transfer.token.lower()
                        if contract == self.base_token.lower() and transfer.amount == base_amount:
                            base_deposit = unmatched_transfers[contract].pop(key, None)

                            matched_transfers.append(base_deposit)
                        elif contract == self.quote_token.lower() and transfer.amount == quote_amount:
                            quote_deposit = remaining_transfers.pop(key, None)
                            matched_transfers.append(quote_deposit)
                else:
                    self.logger.warning(f"No deposits found for mint in transaction {tx.tx_hash} at block {tx.block}")

                
                if liq_transfers["mints"]:
                    for key, transfer in liq_transfers["mints"].items():
                        if transfer.to_address.lower() == self.fee_collector.lower():
                            fee_trf = remaining_transfers.pop(key, None)
                            matched_transfers.append(fee_trf)
                            # Unhandled pool fee
                        else:
                            provider = transfer.to_address.lower()
                            receipt_transfer = remaining_transfers.pop(key, None)
                            matched_transfers.append(receipt_transfer)
                            receipt_amount = receipt_transfer.amount if receipt_transfer else 0
                else:
                    self.logger.warning(f"No mints found for mint in transaction {tx.tx_hash} at block {tx.block}")

                
                events = {}

                mint = Position(
                    timestamp=tx.timestamp,
                    tx_hash=tx.tx_hash,
                    receipt_token= log.contract,
                    receipt_id= 0,
                    amount_base=base_amount,
                    amount_quote=quote_amount,
                    amount_receipt=receipt_amount
                )
                key = mint.generate_content_id()

                liq_in = Liquidity(
                    timestamp=tx.timestamp,
                    tx_hash=tx.tx_hash,
                    pool=log.contract,
                    provider=provider,
                    base_token=self.base_token,
                    amount_base=base_amount,
                    quote_token=self.quote_token,
                    amount_quote=quote_amount,
                    action="add_lp",
                    positions=[mint],
                    transfers=matched_transfers
                )
                key = liq_in.generate_content_id()
                events[key] = liq_in

            elif log.name == "Burn":
                #TODO
                continue
        return events


    

    def handle_swap(self, log: DecodedLog, context: TransactionContext) -> list[PoolSwap]:
        if self.token0 == self.base_token:
            base_amount = log.attributes.get("amount0In") - log.attributes.get("amount0Out")
            quote_amount = log.attributes.get("amount1In") - log.attributes.get("amount1Out")
        elif self.token1 == self.base_token:
            quote_amount = log.attributes.get("amount0In") - log.attributes.get("amount0Out")
            base_amount = log.attributes.get("amount1In") - log.attributes.get("amount1Out")

        direction = self.get_direction(base_amount)

        swaps = []
        swap = PoolSwap(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            pool=log.contract,
            taker= log.attributes.get("to"),
            direction= direction,
            base_token= self.base_token,
            base_amount= base_amount,
            quote_token= self.quote_token,
            quote_amount= quote_amount
        )
        swaps.append(swap)
        return swaps  

    def handle_transfer(self, log: DecodedLog, context: TransactionContext) -> list[Transfer]:
        transfers = []
        transfer = Transfer(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            token=log.contract,
            amount=log.attributes.get("value"),
            from_address=log.attributes.get("from"),
            to_address=log.attributes.get("to")
        )
        transfers.append(transfer)
        return transfers
    
    def transform_log(self, log: DecodedLog, context: TransactionContext) -> list[DomainEvent]:
        events = []
        if log.name == "Transfer":
            events.append(self.handle_transfer(log, context))
        elif log.name == "Swap":
            events.append(self.handle_swap(log, context))
        elif log.name == "Mint":
            events.append(self.handle_mint(log, context))
        elif log.name == "Burn":
            events.append(self.handle_burn(log, context))         

        return events


    def process_transaction(self, transaction: Transaction) -> list[DomainEvent]:
        



