from typing import List

from ...decode.model.evm import EvmAddress
from .base import DomainEvent


class FarmAdd(DomainEvent, tag=True):
    contract: EvmAddress
    farm_id: int
    reward_rate: int
    deposit_token: EvmAddress
    rewarder_address: EvmAddress

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

class FarmSet(DomainEvent, tag=True):
    contract: EvmAddress
    farm_id: int
    reward_rate: int
    rewarder_address: EvmAddress
    overwrite: bool

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

class FarmDeposit(DomainEvent, tag=True):
    contract: EvmAddress
    farm_id: int
    staker: EvmAddress
    amount: int

    def _get_identifying_content(self):
        return {
            "event_type": "farm_deposit",
            "tx_salt": self.tx_hash,
            "contract": self.contract,
            "farm_id": self.farm_id,
            "staker": self.staker,
            "amount": self.amount,
        }

class FarmWithdraw(DomainEvent, tag=True):
    contract: EvmAddress
    farm_id: int
    staker: EvmAddress
    amount: int

    def _get_identifying_content(self):
        return {
            "event_type": "farm_withdraw",
            "tx_salt": self.tx_hash,
            "contract": self.contract,
            "farm_id": self.farm_id,
            "staker": self.staker,
            "amount": self.amount,
        }

class UpdateFarm(DomainEvent, tag=True):
    contract: EvmAddress
    farm_id: int
    last_reward_timestamp: int
    deposit_balance: int
    acc_reward_per_share: int

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

class FarmHarvest(DomainEvent, tag=True):
    contract: EvmAddress
    farm_id: int
    staker: EvmAddress
    amount_received: int
    amount_owed: int

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

class FarmBatchHarvest(DomainEvent, tag=True):
    contract: EvmAddress
    farm_ids: List[int]

    def _get_identifying_content(self):
        return {
            "event_type": "farm_batch_harvest",
            "tx_salt": self.tx_hash,
            "contract": self.contract,
            "farm_ids": self.farm_ids,
        }

class FarmEmergencyWithdraw(DomainEvent, tag=True):
    contract: EvmAddress
    farm_id: int
    staker: EvmAddress
    amount: int

    def _get_identifying_content(self):
        return {
            "event_type": "farm_emergency_withdraw",
            "tx_salt": self.tx_hash,
            "contract": self.contract,
            "farm_id": self.farm_id,
            "staker": self.staker,
            "amount": self.amount,
        }

class FarmSkim(DomainEvent, tag=True):
    contract: EvmAddress
    token: EvmAddress
    to: EvmAddress
    amount: int

    def _get_identifying_content(self):
        return {
            "event_type": "farm_skim",
            "tx_salt": self.tx_hash,
            "contract": self.contract,
            "token": self.token,
            "to": self.to,
            "amount": self.amount,
        }