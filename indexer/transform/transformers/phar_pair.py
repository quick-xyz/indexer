from typing import Union, Optional

from ...decode.model.block import DecodedLog
from ..events.base import DomainEvent
from ..events.transfer import Transfer
from ..events.liquidity import Liquidity
from ..events.trade import Trade
from ..events.fees import Fee
from ..events.rewards import Rewards
from ...utils.logger import get_logger


class PharPairTransformer:
    def __init__(self, contract, token0, token1, base_token):
        self.logger = get_logger(__name__)
        self.contract = contract
        self.token0 = token0
        self.token1 = token1
        self.base = base_token
        self.base_token, self.quote_token = self._get_tokens()

    def _get_tokens(self) -> tuple:
        if self.token0 == self.base:
            base_token = self.token0
            quote_token = self.token1
        elif self.token1 == self.base:
            base_token = self.token1
            quote_token = self.token0

        return base_token, quote_token
    
    def get_amounts(self, log: DecodedLog) -> tuple:
        if self.token0 == self.base:
            base_amount = log.attributes.get("amount0")
            quote_amount = log.attributes.get("amount1")
        elif self.token1 == self.base:
            base_amount = log.attributes.get("amount1")
            quote_amount = log.attributes.get("amount0")

        return base_amount, quote_amount

    def get_direction(self, base_amount: int) -> str:
        if base_amount > 0:
            return "buy"
        else:
            return "sell"
            
    def handle_mint(self, log: DecodedLog, context: DomainEvent) -> list[Liquidity]:
        base_amount, quote_amount = self.get_amounts(log)

        liquidity = []

        mint = Liquidity(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            pool=log.contract,
            provider=log.attributes.get("provider"),
            amount_base=base_amount,
            amount_quote=quote_amount,
            amount_receipt=log.attributes.get("amount_receipt"),
            event_tag="add_lp"
        )
        liquidity.append(mint)
        return liquidity

    def handle_burn(self, log: DecodedLog, context: DomainEvent) -> list[Liquidity]:
        base_amount, quote_amount = self.get_amounts(log)
        liquidity = []
        burn = Liquidity(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            pool=log.contract,
            provider=log.attributes.get("sender"),
            amount_base=base_amount,
            amount_quote=quote_amount,
            amount_receipt=log.attributes.get("amount_receipt"),
            event_tag="remove_lp"
        )
        liquidity.append(burn)
        return liquidity

    def handle_swap(self, log: DecodedLog, context: DomainEvent) -> list[Trade]:
        if self.token0 == self.base:
            base_amount = log.attributes.get("amount0In") - log.attributes.get("amount0Out")
            quote_amount = log.attributes.get("amount1In") - log.attributes.get("amount1Out")
        elif self.token1 == self.base:
            quote_amount = log.attributes.get("amount0In") - log.attributes.get("amount0Out")
            base_amount = log.attributes.get("amount1In") - log.attributes.get("amount1Out")

        direction = self.get_direction(base_amount)

        trades = []
        trade = Trade(
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
        trades.append(trade)
        return trades

    def handle_transfer(self, log: DecodedLog, context: DomainEvent) -> list[Transfer]:
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
    

    def handle_fee(self, log: DecodedLog, context: DomainEvent) -> list[Fee]:
        base_amount, quote_amount = self.get_amounts(log)

        fees = []
        if base_amount > 0:
            base_fee = Fee(
                timestamp=context.timestamp,
                tx_hash=context.tx_hash,
                pool=log.contract,
                fee_type='swap',
                payer=log.attributes.get("sender"),
                token= self.base_token,
                fee_amount=base_amount
            )
            fees.append(base_fee)

        if quote_amount > 0:
            quote_fee = Fee(
                timestamp=context.timestamp,
                tx_hash=context.tx_hash,
                pool=log.contract,
                fee_type='swap',
                payer=log.attributes.get("sender"),
                token= self.quote_token,
                fee_amount=quote_amount
            )
            fees.append(quote_fee)

        return fees
    
    def handle_claim(self, log: DecodedLog, context: DomainEvent) -> list[Rewards]:
        base_amount, quote_amount = self.get_amounts(log)

        rewards = []
        if base_amount > 0:
            base_reward = Rewards(
                timestamp=context.timestamp,
                tx_hash=context.tx_hash,
                contract=log.contract,
                recipient=log.attributes.get("recipient"),
                token= self.base_token,
                amount=base_amount,
                event_tag="claim_fees"
            )
            rewards.append(base_reward)

        if quote_amount > 0:
            quote_reward = Rewards(

                timestamp=context.timestamp,
                tx_hash=context.tx_hash,
                contract=log.contract,
                recipient=log.attributes.get("recipient"),
                token= self.quote_token,
                amount=quote_amount,
                event_tag="claim_fees"
            )
            rewards.append(quote_reward)

        return rewards

    
    
    def transform_log(self, log: DecodedLog, context: DomainEvent) -> list[DomainEvent]:
        events = []
        if log.name == "Transfer":
            events.append(self.handle_transfer(log, context))
        elif log.name == "Swap":
            events.append(self.handle_swap(log, context))
        elif log.name == "Mint":
            events.append(self.handle_mint(log, context))
        elif log.name == "Burn":
            events.append(self.handle_burn(log, context))
        elif log.name == "Fees":
            events.append(self.handle_fee(log, context))
        elif log.name == "Claim":
            events.append(self.handle_claim(log, context))           

        return events