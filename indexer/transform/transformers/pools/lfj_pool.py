from typing import List, Dict, Tuple, Optional, Literal

from ..base import BaseTransformer
from ....decode.model.block import DecodedLog, Transaction
from ....decode.model.types import EvmAddress
from ...events.base import DomainEvent, TransactionContext
from ...events.transfer import Transfer
from ...events.liquidity import Liquidity, Position
from ...events.fees import Fee
from ...events.trade import PoolSwap
from ....utils.logger import get_logger

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

class LfjPoolTransformer(BaseTransformer):
    def __init__(self, contract, token0, token1, base_token, fee_collector):
        self.logger = get_logger(__name__)
        self.contract = contract
        self.token0 = token0
        self.token1 = token1
        self.base_token = base_token
        self.quote_token = token1 if token0 == base_token else token0
        self.fee_collector = fee_collector

    
    def get_amounts(self, log: DecodedLog) -> tuple[int, int]:
        amount0 = log.attributes.get("amount0")
        amount1 = log.attributes.get("amount1")

        if self.token0 == self.base_token:
            return amount0, amount1
        else:
            return amount1, amount0


    def get_direction(self, base_amount: int) -> str:
        return "buy" if base_amount > 0 else "sell"
    

    def process_transfers(self, logs: List[DecodedLog], tx: Transaction) -> dict[str,Transfer]:
        transfers = {}

        for log in logs:
            if log.name == "Transfer":
                transfer = Transfer(
                    timestamp=tx.timestamp,
                    tx_hash=tx.tx_hash,
                    from_address=log.attributes.get("from").lower(),
                    to_address=log.attributes.get("to").lower(),
                    token=log.contract,
                    amount=log.attributes.get("value"),
                )
                key = transfer.generate_content_id()
                
                transfers[key] = transfer
                
        return transfers

    
    def get_liquidity_transfers(self, tx: Transaction) -> Dict[str, List[Tuple[str,Transfer]]]:
        liq_transfers = dict.fromkeys(["mints", "burns", "receipt_transfers", "deposits", "withdrawals", "underlying_transfers"])

        for key, transfer in tx.transfers.unmatched.items():
            if transfer.contract.lower() == self.contract.lower():
                if transfer.from_address == ZERO_ADDRESS:
                    liq_transfers["mints"] = (key, transfer)
                elif transfer.to_address == ZERO_ADDRESS:
                    liq_transfers["burns"] = (key, transfer)
                else:
                    liq_transfers["receipt_transfers"] = (key, transfer)
            elif transfer.contract.lower() == self.base_token.lower() or transfer.contract.lower() == self.quote_token.lower():
                if transfer.to_address == self.contract.lower():
                    liq_transfers["deposits"] = (key, transfer)
                elif transfer.from_address == self.contract.lower():
                    liq_transfers["withdrawals"] = (key, transfer)
                else:
                    liq_transfers["underlying_transfers"] = (key, transfer)

        return liq_transfers
    
    def handle_mint(self, log: DecodedLog, context: TransactionContext) -> list[DomainEvent]:
        base_amount, quote_amount = self.get_amounts(log)

        remaining_transfers = context.transaction.transfers.unmatched.copy()
        liq_transfers = self
    
    def process_events(self, logs: List[DecodedLog], tx: Transaction) -> Dict[str,DomainEvent]:
        events = {}

        for log in logs:
            if log.name == "Swap":
                #TODO
                continue

            elif log.name == "Mint":
                base_amount, quote_amount = self.get_amounts(log)
                
                remaining_transfers = tx.transfers.unmatched.copy()
                liq_transfers = self.get_liquidity_transfers(tx)
                matched_transfers = []

                # look for deposit transfers
                for key, transfer in liq_transfers["deposits"]:
                    if transfer.token.lower() == self.base_token.lower() and transfer.amount == base_amount:
                        base_deposit = remaining_transfers.pop(key, None)
                        matched_transfers.append(base_deposit)
                    elif transfer.token.lower() == self.quote_token.lower() and transfer.amount == quote_amount:
                        quote_deposit = remaining_transfers.pop(key, None)
                        matched_transfers.append(quote_deposit)

                
                # look for receipt mints
                for key, transfer in liq_transfers["mints"]:
                    if transfer.to_address.lower() == self.fee_collector.lower():
                        fee_trf = remaining_transfers.pop(key, None)
                        matched_transfers.append(fee_trf)
                        # Unhandled pool fee
                    else:
                        provider = transfer.to_address.lower()
                        receipt_transfer = remaining_transfers.pop(key, None)
                        matched_transfers.append(receipt_transfer)
                        receipt_amount = receipt_transfer.amount if receipt_transfer else 0

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
        



