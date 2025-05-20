from typing import Union, List

from ...decode.model.block import DecodedLog
from ..events.base import DomainEvent, TransactionContext
from ..events.transfer import Transfer
from ..events.liquidity import Liquidity
from ..events.bin_liquidity import BinLiquidity
from ..events.bin_transfer import BinTransfer
from ..events.swap import Swap
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

    def unpack_amounts(self, bytes) -> tuple:
        if self.token_x == self.base:
            base_amount, quote_amount = decode_amounts(bytes)
        elif self.token_y == self.base:
            quote_amount, base_amount = decode_amounts(bytes)

        return base_amount, quote_amount
    
    def handle_deposit(self, log: DecodedLog, context: TransactionContext) -> List[BinLiquidity]:

        bins = log.attributes.get("ids")
        amounts = log.attributes.get("amounts")
        liquidity = []

        for i in bins:
            base_amount, quote_amount = self.unpack_amounts(amounts[i])

            bin_liquidity = BinLiquidity(
                timestamp=context.timestamp,
                tx_hash=context.tx_hash,
                pool=log.contract,
                id=i,
                provider=log.attributes.get("sender"),
                amount_base=base_amount,
                amount_quote=quote_amount,
                event_tag="add_lp"
            )
            liquidity.append(bin_liquidity)

        return liquidity

    def handle_withdraw(self, log: DecodedLog, context: TransactionContext) -> List[BinLiquidity]:
        bins = log.attributes.get("ids")
        amounts = log.attributes.get("amounts")
        liquidity = []
        
        for i in bins:
            base_amount, quote_amount = self.unpack_amounts(amounts[i])

            bin_liquidity = BinLiquidity(
                timestamp=context.timestamp,
                tx_hash=context.tx_hash,
                pool=log.contract,
                id=i,
                provider=log.attributes.get("sender"),
                amount_base=base_amount,
                amount_quote=quote_amount,
                event_tag="remove_lp"
            )
            liquidity.append(bin_liquidity)

        return liquidity

    def handle_swap(self, log: DecodedLog, context: TransactionContext) -> List[DomainEvent]:
        base_amount_in, quote_amount_in = self.unpack_amounts(log.attributes.get("amountsIn"))
        base_amount_out, quote_amount_out = self.unpack_amounts(log.attributes.get("amountsOut"))
        base_amount_fee, quote_amount_fee = self.unpack_amounts(log.attributes.get("totalFees"))

        base_amount = base_amount_in - base_amount_out
        quote_amount = quote_amount_in - quote_amount_out
        direction = self.get_direction(base_amount)
        events = []

        swap = Swap(
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
        events.append(swap)

        if base_amount_fee > 0:
            base_fee = Fee(
                timestamp=context.timestamp,
                tx_hash=context.tx_hash,
                pool=log.contract,
                fee_type='swap',
                payer=log.attributes.get("sender"),
                token= self.base_token,
                fee_amount=base_amount_fee
            )
            events.append(base_fee)

        if quote_amount_fee > 0:
            quote_fee = Fee(
                timestamp=context.timestamp,
                tx_hash=context.tx_hash,
                pool=log.contract,
                fee_type='swap',
                payer=log.attributes.get("sender"),
                token= self.quote_token,
                fee_amount=base_amount_fee
            )
            events.append(quote_fee)

        return events

    def handle_transfer(self, log: DecodedLog, context: TransactionContext) -> List[Transfer]:
        bins = log.attributes.get("ids")
        amounts = log.attributes.get("amounts")
        sender = log.attributes.get("sender")
        from_address=log.attributes.get("from"),
        to_address=log.attributes.get("to")

        transfers = []
        
        for i in bins:
            base_amount, quote_amount = self.unpack_amounts(amounts[i])

            if base_amount > 0:
                bin_transfer = BinTransfer(
                    timestamp=context.timestamp,
                    tx_hash=context.tx_hash,
                    pool=log.contract,
                    bin=i,                
                    token=self.base_token,
                    amount=base_amount,
                    from_address=from_address,
                    to_address=to_address,
                    event_tag="transfer"
                )
                transfers.append(bin_transfer)

            if quote_amount > 0:
                bin_transfer = BinTransfer(
                    timestamp=context.timestamp,
                    tx_hash=context.tx_hash,
                    pool=log.contract,
                    bin=i,                
                    token=self.quote_token,
                    amount=quote_amount,
                    from_address=from_address,
                    to_address=to_address,
                    event_tag="transfer"
                )
                transfers.append(bin_transfer)

        return transfers 
    
    def handle_comp_fee(self, log: DecodedLog, context: TransactionContext) -> List[Fee]:
        base_amount, quote_amount = self.unpack_amounts(log.attributes.get("totalFees"))
        #base_fee_protocol, quote_fee_protocol = self.unpack_amounts(log.attributes.get("protocolFees"))
        #bin_id = log.attributes.get("id")

        fees = []
        if base_amount > 0:
            base_fee = Fee(
                timestamp=context.timestamp,
                tx_hash=context.tx_hash,
                pool=log.contract,
                fee_type='composition',
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
                fee_type='composition',
                payer=log.attributes.get("sender"),
                token= self.quote_token,
                fee_amount=quote_amount
            )
            fees.append(quote_fee)

        return fees
    
    
    def transform_log(self, log: DecodedLog, context: TransactionContext) -> list[DomainEvent]:
        events = []
        if log.name == "TransferBatch":
            events.append(self.handle_transfer(log, context))
        elif log.name == "Swap":
            events.append(self.handle_swap(log, context))
        elif log.name == "DepositedToBins":
            events.append(self.handle_deposit(log, context))
        elif log.name == "WithdrawnFromBins":
            events.append(self.handle_withdraw(log, context))
        elif log.name == "CompositionFees":
            events.append(self.handle_comp_fee(log, context))

        return events
        

    



