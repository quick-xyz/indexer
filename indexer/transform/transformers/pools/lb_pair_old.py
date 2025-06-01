from typing import List

from ....decode import DecodedLog
from ...events.base import DomainEvent, TransactionContext
from ...events.transfer import Transfer, TransferIds
from ...events.liquidity import Liquidity, Position
from ...events.trade import PoolSwap
from ....utils.logger import get_logger
from ....utils.lb_byte32_decoder import decode_amounts


class LbPairTransformer:
    def __init__(self, contract, token_x, token_y, base_token):
        self.logger = get_logger(__name__)
        self.contract = contract
        self.token_x = token_x
        self.token_y = token_y
        self.base_token = base_token
        self.quote_token = token_y if token_x == base_token else token_x



    
    def handle_deposit(self, log: DecodedLog, context: TransactionContext) -> List[Liquidity]:

        bins = log.attributes.get("ids")
        amounts = log.attributes.get("amounts")
        positions = []
        liquidity = []
        sum_base = 0
        sum_quote = 0

        for i in bins:
            base_amount, quote_amount = self.unpack_amounts(amounts[i])

            bin_liquidity = Position(
                receipt_token=log.contract,
                receipt_id=i,
                amount_base=base_amount,
                amount_quote=quote_amount,
            )
            sum_base += base_amount
            sum_quote += quote_amount

            positions.append(bin_liquidity)

        liq = Liquidity(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            pool=log.contract,
            provider=log.attributes.get("to"),
            base_token=self.base_token,
            amount_base=sum_base,
            quote_token=self.quote_token,
            amount_quote=sum_quote,
            liquidity_type="add_lp",
            positions=positions
        )
        liquidity.append(liq)
        return liquidity

    def handle_withdraw(self, log: DecodedLog, context: TransactionContext) -> List[Liquidity]:
        bins = log.attributes.get("ids")
        amounts = log.attributes.get("amounts")
        positions = []
        liquidity = []
        sum_base = 0
        sum_quote = 0
        
        for i in bins:
            base_amount, quote_amount = self.unpack_amounts(amounts[i])

            bin_liquidity = Position(
                receipt_token=log.contract,
                receipt_id=i,
                amount_base=-(base_amount),
                amount_quote=-(quote_amount),
            )
            sum_base += base_amount
            sum_quote += quote_amount

            positions.append(bin_liquidity)

        liq = Liquidity(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            pool=log.contract,
            provider=log.attributes.get("to"),
            base_token=self.base_token,
            amount_base=-(sum_base),
            quote_token=self.quote_token,
            amount_quote=-(sum_quote),
            liquidity_type="remove_lp",
            positions=positions
        )
        liquidity.append(liq)
        return liquidity

    def handle_swap(self, log: DecodedLog, context: TransactionContext) -> List[DomainEvent]:
        base_amount_in, quote_amount_in = self.unpack_amounts(log.attributes.get("amountsIn"))
        base_amount_out, quote_amount_out = self.unpack_amounts(log.attributes.get("amountsOut"))

        base_amount = base_amount_in - base_amount_out
        quote_amount = quote_amount_in - quote_amount_out
        direction = self.get_direction(base_amount)
        events = []

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
        events.append(swap)

        return events

 
        
    def transform_log(self, log: DecodedLog, context: TransactionContext) -> list[DomainEvent]:
        events = []

        elif log.name == "Swap": #0xfe8360c9a854b807ec65554738c9a9b34e416347bc77b240901e40b2249e4456
            events.append(self.handle_swap(log, context))
        elif log.name == "DepositedToBins": #0x6ed579bb7bf84583ffcde9eb005f0bbcb85d20dff7591b906ddafdc6c4f85f6e
            events.append(self.handle_deposit(log, context))
        elif log.name == "WithdrawnFromBins": #0x06cd109690c39408a5882ba47703146dd1cffe2ca8bce98a8b91349b27731d03
            events.append(self.handle_withdraw(log, context))

        return events
        

    



