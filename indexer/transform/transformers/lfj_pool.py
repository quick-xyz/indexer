from typing import Union

from ...decode.model.block import DecodedLog
from ..events.base import DomainEvent
from ..events.transfer import Transfer
from ..events.liquidity import Liquidity
from ..events.trade import Trade
from ...utils.logger import get_logger


class LfjPoolTransformer:
    def __init__(self, contract, token0, token1, base_token):
        self.logger = get_logger(__name__)
        self.contract = contract
        self.token0 = token0
        self.token1 = token1
        self.base = base_token


    def handle_mint(self, log: DecodedLog, context: DomainEvent) -> Liquidity:
        if self.token0 == self.base:
            base_amount = log.attributes.get("amount0")
            quote_amount = log.attributes.get("amount1")
        elif self.token1 == self.base:
            base_amount = log.attributes.get("amount1")
            quote_amount = log.attributes.get("amount0")
        
        return Liquidity(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            pool=log.contract,
            provider=log.attributes.get("provider"),
            amount_base=base_amount,
            amount_quote=quote_amount,
            amount_receipt=log.attributes.get("amount_receipt"),
            event_tag="add_lp"
        )

    def handle_burn(self, log: DecodedLog, context: DomainEvent) -> Liquidity:
        if self.token0 == self.base:
            base_amount = log.attributes.get("amount0")
            quote_amount = log.attributes.get("amount1")
        elif self.token1 == self.base:
            base_amount = log.attributes.get("amount1")
            quote_amount = log.attributes.get("amount0")
        
        return Liquidity(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            pool=log.contract,
            provider=log.attributes.get("sender"),
            amount_base=base_amount,
            amount_quote=quote_amount,
            amount_receipt=log.attributes.get("amount_receipt"),
            event_tag="remove_lp"
        )


    def handle_swap(self, log: DecodedLog, context: DomainEvent) -> Trade:
        if self.token0 == self.base:
            base_token = self.token0
            base_amount = log.attributes.get("amount0In") - log.attributes.get("amount0Out")
            quote_token = self.token1
            quote_amount = log.attributes.get("amount1In") - log.attributes.get("amount1Out")
        elif self.token1 == self.base:
            quote_token = self.token0
            quote_amount = log.attributes.get("amount0In") - log.attributes.get("amount0Out")
            base_token = self.token1
            base_amount = log.attributes.get("amount1In") - log.attributes.get("amount1Out")

        if base_amount > 0:
            direction = "buy"
        else:
            direction = "sell"

        return Trade(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            pool=log.contract,
            taker= log.attributes.get("sender"),
            direction= direction,
            base_token= base_token,
            base_amount= base_amount,
            quote_token= quote_token,
            quote_amount= quote_amount
        )   

    def handle_transfer(self, log: DecodedLog, context: DomainEvent) -> Transfer:
        return Transfer(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            token=log.contract,
            amount=log.attributes.get("value"),
            from_address=log.attributes.get("from"),
            to_address=log.attributes.get("to")
        )
    
    def transform_log(self, log: DecodedLog, context: DomainEvent) -> Union[Liquidity, Trade, Transfer]:
        if log.name == "Transfer":
            obj = self.handle_transfer(log, context)
        elif log.name == "Swap":
            obj = self.handle_swap(log, context)
        elif log.name == "Mint":
            obj = self.handle_mint(log, context)
        elif log.name == "Burn":
            obj = self.handle_burn(log, context)

        return obj

        

    



