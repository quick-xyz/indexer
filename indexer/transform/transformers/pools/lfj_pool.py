from ....decode.model.block import DecodedLog
from ...events.base import DomainEvent, TransactionContext
from ...events.transfer import Transfer
from ...events.liquidity import Liquidity
from ...events.trade import PoolSwap
from ....utils.logger import get_logger


class LfjPoolTransformer:
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

    def handle_mint(self, log: DecodedLog, context: TransactionContext) -> list[Liquidity]:
        base_amount, quote_amount = self.get_amounts(log)

        liquidity = []

        mint = Liquidity(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            pool=log.contract,
            provider=log.attributes.get("sender"),
            base_token=self.base_token,
            amount_base=base_amount,
            quote_token=self.quote_token,
            amount_quote=quote_amount,
            liquidity_type="add_lp",
        )
        liquidity.append(mint)
        return liquidity

    def handle_burn(self, log: DecodedLog, context: TransactionContext) -> list[Liquidity]:
        base_amount, quote_amount = self.get_amounts(log)
        liquidity = []
        burn = Liquidity(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            pool=log.contract,
            provider=log.attributes.get("to"),
            base_token=self.base_token,
            amount_base= -(base_amount),
            quote_token=self.quote_token,
            amount_quote= -(quote_amount),
            liquidity_type="remove_lp"
        )
        liquidity.append(burn)
        return liquidity


    def handle_swap(self, log: DecodedLog, context: TransactionContext) -> list[PoolSwap]:
        if self.token0 == self.base:
            base_amount = log.attributes.get("amount0In") - log.attributes.get("amount0Out")
            quote_amount = log.attributes.get("amount1In") - log.attributes.get("amount1Out")
        elif self.token1 == self.base:
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

        

    



