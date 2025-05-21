from typing import Union, Optional

from ....decode.model.block import DecodedLog
from ...events.base import DomainEvent, TransactionContext
from ...events.transfer import Transfer, TransferIds
from ...events.liquidity import Liquidity, Position
from ...events.trade import PoolSwap
from ...events.rewards import Reward, Rewards
from ....utils.logger import get_logger


class PharClpoolTransformer:
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

        positions = []
        liquidity = []

        position = Position(
            receipt_token=log.contract,
            receipt_id=0,
            amount_base=base_amount,
            amount_quote=quote_amount,
            amount_receipt=log.attributes.get("amount"),
            custodian=log.attributes.get("owner")
        )
        positions.append(position)

        mint = Liquidity(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            pool=log.contract,
            provider=log.attributes.get("owner"),
            base_token=self.base_token,
            amount_base=base_amount,
            quote_token=self.quote_token,
            amount_quote=quote_amount,
            liquidity_type="add_lp",
            positions=positions
        )
        liquidity.append(mint)
        return liquidity

    def handle_burn(self, log: DecodedLog, context: TransactionContext) -> list[Liquidity]:
        base_amount, quote_amount = self.get_amounts(log)

        positions = []
        liquidity = []

        position = Position(
            receipt_token=log.contract,
            receipt_id=0,
            amount_base=-(base_amount),
            amount_quote=-(quote_amount),
            amount_receipt=-(log.attributes.get("amount")),
            custodian=log.attributes.get("owner")
        )
        positions.append(position)

        burn = Liquidity(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            pool=log.contract,
            provider=log.attributes.get("owner"),
            base_token=self.base_token,
            amount_base=-(base_amount),
            quote_token=self.quote_token,
            amount_quote=-(quote_amount),
            liquidity_type="remove_lp",
            positions=positions
        )
        liquidity.append(burn)
        return liquidity

    def handle_swap(self, log: DecodedLog, context: TransactionContext) -> list[PoolSwap]:
        base_amount, quote_amount = self.get_amounts(log)
        direction = self.get_direction(base_amount)

        swaps = []

        swap = PoolSwap(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            pool=log.contract,
            taker= log.attributes.get("recipient"),
            direction= direction,
            base_token= self.base_token,
            base_amount= base_amount,
            quote_token= self.quote_token,
            quote_amount= quote_amount
        )
        swaps.append(swap)
        return swaps

    def handle_claim(self, log: DecodedLog, context: TransactionContext) -> list[Rewards]:
        base_amount, quote_amount = self.get_amounts(log)

        base_rewards = []
        quote_rewards = []
        rewards = []

        if base_amount > 0:
            base_reward = Reward(
                reward_token=self.base_token,
                amount=base_amount,
                reward_type="claim_fees"
            )
            base_rewards.append(base_reward)
            rewards_base = Rewards(
                timestamp=context.timestamp,
                tx_hash=context.tx_hash,
                contract=log.contract,
                recipient=log.attributes.get("recipient"),
                token= self.base_token,
                amount=base_amount,
                rewards=base_rewards
            )
            rewards.append(rewards_base)

        if quote_amount > 0:
            quote_reward = Reward(
                reward_token=self.quote_token,
                amount=quote_amount,
                reward_type="claim_fees"
            )
            quote_rewards.append(quote_reward)
            rewards_quote = Rewards(
                timestamp=context.timestamp,
                tx_hash=context.tx_hash,
                contract=log.contract,
                recipient=log.attributes.get("recipient"),
                token= self.quote_token,
                amount=quote_amount,
                rewards=quote_rewards
            )
            rewards.append(rewards_quote)

        return rewards  

    def transform_log(self, log: DecodedLog, context: TransactionContext) -> list[DomainEvent]:
        events = []
        if log.name == "Swap":
            events.append(self.handle_swap(log, context))
        elif log.name == "Mint":
            events.append(self.handle_mint(log, context))
        elif log.name == "Burn":
            events.append(self.handle_burn(log, context))
        elif log.name == "Collect":
            events.append(self.handle_claim(log, context))           

        return events