from typing import Union

from ...decode.model.block import DecodedLog
from ..events.base import DomainEvent
from ..events.transfer import Transfer
from ..events.liquidity import Liquidity
from ..events.trade import Trade
from ..events.fees import Fee
from ..events.rewards import Rewards
from ...utils.logger import get_logger
from ...utils.lb_byte32_decoder import decode_amounts


class LbPairTransformer:
    def __init__(self, contract, token_x, token_y, base_token):
        self.logger = get_logger(__name__)
        self.contract = contract
        self.token_x = token_x
        self.token_y = token_y
        self.base = base_token
        self.base_token, self.quote_token = self._get_tokens()

    def _get_tokens(self) -> tuple:
        if self.token_x == self.base:
            base_token = self.token_x
            quote_token = self.token_y
        elif self.token_y == self.base:
            base_token = self.token_y
            quote_token = self.token_x

        return base_token, quote_token

    def get_direction(self, base_amount: int) -> str:
        if base_amount > 0:
            return "buy"
        else:
            return "sell"

    def handle_mint(self, log: DecodedLog, context: DomainEvent) -> Liquidity:
        if self.token_x == self.base:
            base_amount, quote_amount = decode_amounts(log.attributes.get("amount0"))
        elif self.token_y == self.base:
            quote_amount, base_amount = decode_amounts(log.attributes.get("amount0"))
        
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
        if self.token_x == self.base:
            base_amount = log.attributes.get("amount0")
            quote_amount = log.attributes.get("amount1")
        elif self.token_y == self.base:
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


    def unpack_amounts(self, bytes) -> tuple:
        if self.token_x == self.base:
            base_amount, quote_amount = decode_amounts(bytes)
        elif self.token_y == self.base:
            quote_amount, base_amount = decode_amounts(bytes)

        return base_amount, quote_amount

    def handle_swap(self, log: DecodedLog, context: DomainEvent) -> Trade:
        base_amount_in, quote_amount_in = self.unpack_amounts(log.attributes.get("amountsIn"))
        base_amount_out, quote_amount_out = self.unpack_amounts(log.attributes.get("amountsOut"))
        #base_fee, quote_fee = self.unpack_amounts(log.attributes.get("totalFees"))

        base_amount = base_amount_in - base_amount_out
        quote_amount = quote_amount_in - quote_amount_out


        direction = self.get_direction(base_amount)

        return Trade(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            pool=log.contract,
            taker= log.attributes.get("sender"),
            direction= direction,
            base_token= self.base_token,
            base_amount= base_amount,
            quote_token= self.quote_token,
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
    

    def handle_fee(self, log: DecodedLog, context: DomainEvent) -> Fee:
        base_fee, quote_fee = self.unpack_amounts(log.attributes.get("totalFees"))
        
        #base_fee_protocol, quote_fee_protocol = self.unpack_amounts(log.attributes.get("protocolFees"))
        #bin_id = log.attributes.get("id")

        return Fee(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            taker=log.attributes.get("sender"),
            base_token=self.base_token,
            base_amount=base_fee,
            quote_token=self.quote_token,
            quote_amount=quote_fee
        )
    
    def handle_claim(self, log: DecodedLog, context: DomainEvent) -> Trade:
        if self.token_x == self.base:
            base_amount = log.attributes.get("amount0")
            quote_amount = log.attributes.get("amount1")
        elif self.token_y == self.base:
            base_amount = log.attributes.get("amount1")
            quote_amount = log.attributes.get("amount0")

        return Rewards(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            taker=log.attributes.get("sender"),
            base_token=self.base_token,
            base_amount=base_amount,
            quote_token=self.quote_token,
            quote_amount=quote_amount
        )
    
    
    def transform_log(self, log: DecodedLog, context: DomainEvent) -> Union[Liquidity, Trade, Transfer]:
        if log.name == "Swap":
            obj = self.handle_swap(log, context)
        elif log.name == "TransferBatch":
            obj = self.handle_transfer(log, context)
        elif log.name == "DepositedToBins":
            obj = self.handle_deposit(log, context)
        elif log.name == "WithdrawnFromBins":
            obj = self.handle_withdraw(log, context)
        elif log.name == "CompositionFees":
            obj = self.handle_fee(log, context)      
        return obj

        

    



