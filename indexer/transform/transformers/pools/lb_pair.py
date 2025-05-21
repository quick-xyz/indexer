from typing import List

from ....decode.model.block import DecodedLog
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

    def handle_transfer(self, log: DecodedLog, context: TransactionContext) -> List[Transfer]:
        bins = log.attributes.get("ids")
        amounts = log.attributes.get("amounts")
        from_address=log.attributes.get("from"),
        to_address=log.attributes.get("to")

        transferids = []
        transfers = []
        sum_transfers = 0
        
        for i in bins:    
            trf = TransferIds(
                id=i,
                amount=amounts[i]
            )
            sum_transfers += amounts[i]
            transferids.append(trf)           
            
        transfer = Transfer(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            token=self.base_token,
            amount=sum_transfers,
            from_address=from_address,
            to_address=to_address,
            transfer_type="transfer_batch",
            batch=transferids
        )
        transfers.append(transfer)

        return transfers 
        
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

        return events
        

    



