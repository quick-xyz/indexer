# indexer/types/model/farm.py

from typing import List

from ..new import EvmAddress, EvmHash
from .base import DomainEvent, Signal


# Signals
class FarmAddSignal(Signal, tag=True):
    contract: EvmAddress
    farm_id: int
    reward_rate: int
    deposit_token: EvmAddress
    rewarder_address: EvmAddress

class FarmSetSignal(Signal, tag=True):
    contract: EvmAddress
    farm_id: int
    reward_rate: int
    rewarder_address: EvmAddress
    overwrite: bool

class FarmDepositSignal(Signal, tag=True):
    contract: EvmAddress
    farm_id: int
    staker: EvmAddress
    amount: str

class FarmWithdrawSignal(Signal, tag=True):
    contract: EvmAddress
    farm_id: int
    staker: EvmAddress
    amount: str

class UpdateFarmSignal(Signal, tag=True):
    contract: EvmAddress
    farm_id: int
    last_reward_timestamp: int
    deposit_balance: str
    acc_reward_per_share: str

class FarmHarvestSignal(Signal, tag=True):
    contract: EvmAddress
    farm_id: int
    staker: EvmAddress
    amount_received: str
    amount_owed: str

class FarmBatchHarvestSignal(Signal, tag=True):
    contract: EvmAddress
    farm_ids: List[int]

class FarmEmergencyWithdrawSignal(Signal, tag=True):
    contract: EvmAddress
    farm_id: int
    staker: EvmAddress
    amount: str

class FarmSkimSignal(Signal, tag=True):
    contract: EvmAddress
    token: EvmAddress
    to: EvmAddress
    amount: str


# Domain Events
class FarmAdd(DomainEvent, tag=True, kw_only=True):
    contract: EvmAddress
    farm_id: int
    reward_rate: int
    deposit_token: EvmAddress
    rewarder_address: EvmAddress

    @classmethod
    def from_signal(cls, signal: FarmAddSignal, timestamp: int, tx_hash: EvmHash):
        return cls(
            timestamp=timestamp,
            tx_hash=tx_hash,
            contract=signal.contract,
            farm_id=signal.farm_id,
            reward_rate=signal.reward_rate,
            deposit_token=signal.deposit_token,
            rewarder_address=signal.rewarder_address,
        )

    def _get_identifying_content(self):
        return {
            "event_type": "farm_add",
            "tx_salt": self.tx_hash,
            "contract": self.contract,
            "farm_id": self.farm_id,
            "reward_rate": self.reward_rate,
            "deposit_token": self.deposit_token,
            "rewarder_address": self.rewarder_address,
        }

class FarmSet(DomainEvent, tag=True, kw_only=True):
    contract: EvmAddress
    farm_id: int
    reward_rate: int
    rewarder_address: EvmAddress
    overwrite: bool

    @classmethod
    def from_signal(cls, signal: FarmSetSignal, timestamp: int, tx_hash: EvmHash):
        return cls(
            timestamp=timestamp,
            tx_hash=tx_hash,
            contract=signal.contract,
            farm_id=signal.farm_id,
            reward_rate=signal.reward_rate,
            rewarder_address=signal.rewarder_address,
            overwrite=signal.overwrite,
        )

    def _get_identifying_content(self):
        return {
            "event_type": "farm_set",
            "tx_salt": self.tx_hash,
            "contract": self.contract,
            "farm_id": self.farm_id,
            "reward_rate": self.reward_rate,
            "rewarder_address": self.rewarder_address,
            "overwrite": self.overwrite,
        }

class FarmDeposit(DomainEvent, tag=True, kw_only=True):
    contract: EvmAddress
    farm_id: int
    staker: EvmAddress
    amount: str

    @classmethod
    def from_signal(cls, signal: FarmDepositSignal, timestamp: int, tx_hash: EvmHash):
        return cls(
            timestamp=timestamp,
            tx_hash=tx_hash,
            contract=signal.contract,
            farm_id=signal.farm_id,
            staker=signal.staker,
            amount=signal.amount,
        )

    def _get_identifying_content(self):
        return {
            "event_type": "farm_deposit",
            "tx_salt": self.tx_hash,
            "contract": self.contract,
            "farm_id": self.farm_id,
            "staker": self.staker,
            "amount": self.amount,
        }

class FarmWithdraw(DomainEvent, tag=True, kw_only=True):
    contract: EvmAddress
    farm_id: int
    staker: EvmAddress
    amount: str

    @classmethod
    def from_signal(cls, signal: FarmWithdrawSignal, timestamp: int, tx_hash: EvmHash):
        return cls(
            timestamp=timestamp,
            tx_hash=tx_hash,
            contract=signal.contract,
            farm_id=signal.farm_id,
            staker=signal.staker,
            amount=signal.amount,
        )

    def _get_identifying_content(self):
        return {
            "event_type": "farm_withdraw",
            "tx_salt": self.tx_hash,
            "contract": self.contract,
            "farm_id": self.farm_id,
            "staker": self.staker,
            "amount": self.amount,
        }

class UpdateFarm(DomainEvent, tag=True, kw_only=True):
    contract: EvmAddress
    farm_id: int
    last_reward_timestamp: int
    deposit_balance: str
    acc_reward_per_share: str

    @classmethod
    def from_signal(cls, signal: UpdateFarmSignal, timestamp: int, tx_hash: EvmHash):
        return cls(
            timestamp=timestamp,
            tx_hash=tx_hash,
            contract=signal.contract,
            farm_id=signal.farm_id,
            last_reward_timestamp=signal.last_reward_timestamp,
            deposit_balance=signal.deposit_balance,
            acc_reward_per_share=signal.acc_reward_per_share,
        )

    def _get_identifying_content(self):
        return {
            "event_type": "update_farm",
            "tx_salt": self.tx_hash,
            "contract": self.contract,
            "farm_id": self.farm_id,
            "last_reward_timestamp": self.last_reward_timestamp,
            "deposit_balance": self.deposit_balance,
            "acc_reward_per_share": self.acc_reward_per_share,
        }

class FarmHarvest(DomainEvent, tag=True, kw_only=True):
    contract: EvmAddress
    farm_id: int
    staker: EvmAddress
    amount_received: str
    amount_owed: str

    @classmethod
    def from_signal(cls, signal: FarmHarvestSignal, timestamp: int, tx_hash: EvmHash):
        return cls(
            timestamp=timestamp,
            tx_hash=tx_hash,
            contract=signal.contract,
            farm_id=signal.farm_id,
            staker=signal.staker,
            amount_received=signal.amount_received,
            amount_owed=signal.amount_owed,
        )

    def _get_identifying_content(self):
        return {
            "event_type": "farm_harvest",
            "tx_salt": self.tx_hash,
            "contract": self.contract,
            "farm_id": self.farm_id,
            "staker": self.staker,
            "amount_received": self.amount_received,
            "amount_owed": self.amount_owed,
        }

class FarmBatchHarvest(DomainEvent, tag=True, kw_only=True):
    contract: EvmAddress
    farm_ids: List[int]

    @classmethod
    def from_signal(cls, signal: FarmBatchHarvestSignal, timestamp: int, tx_hash: EvmHash):
        return cls(
            timestamp=timestamp,
            tx_hash=tx_hash,
            contract=signal.contract,
            farm_ids=signal.farm_ids,
        )

    def _get_identifying_content(self):
        return {
            "event_type": "farm_batch_harvest",
            "tx_salt": self.tx_hash,
            "contract": self.contract,
            "farm_ids": sorted(self.farm_ids),  # Sort for deterministic ID
        }

class FarmEmergencyWithdraw(DomainEvent, tag=True, kw_only=True):
    contract: EvmAddress
    farm_id: int
    staker: EvmAddress
    amount: str

    @classmethod
    def from_signal(cls, signal: FarmEmergencyWithdrawSignal, timestamp: int, tx_hash: EvmHash):
        return cls(
            timestamp=timestamp,
            tx_hash=tx_hash,
            contract=signal.contract,
            farm_id=signal.farm_id,
            staker=signal.staker,
            amount=signal.amount,
        )

    def _get_identifying_content(self):
        return {
            "event_type": "farm_emergency_withdraw",
            "tx_salt": self.tx_hash,
            "contract": self.contract,
            "farm_id": self.farm_id,
            "staker": self.staker,
            "amount": self.amount,
        }

class FarmSkim(DomainEvent, tag=True, kw_only=True):
    contract: EvmAddress
    token: EvmAddress
    to: EvmAddress
    amount: str

    @classmethod
    def from_signal(cls, signal: FarmSkimSignal, timestamp: int, tx_hash: EvmHash):
        return cls(
            timestamp=timestamp,
            tx_hash=tx_hash,
            contract=signal.contract,
            token=signal.token,
            to=signal.to,
            amount=signal.amount,
        )

    def _get_identifying_content(self):
        return {
            "event_type": "farm_skim",
            "tx_salt": self.tx_hash,
            "contract": self.contract,
            "token": self.token,
            "to": self.to,
            "amount": self.amount,
        }