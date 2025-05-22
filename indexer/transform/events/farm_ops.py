from typing import List, Optional

from msgspec import Struct
from datetime import datetime
from ...decode.model.evm import EvmAddress,EvmHash

from .base import DomainEvent

class FarmAdd(DomainEvent, tag=True):
    contract: EvmAddress
    farm_id: int
    reward_rate: int
    deposit_token: EvmAddress
    rewarder_address: EvmAddress

class FarmSet(DomainEvent, tag=True):
    contract: EvmAddress
    farm_id: int
    reward_rate: int
    rewarder_address: EvmAddress
    overwrite: bool

class FarmDeposit(DomainEvent, tag=True):
    contract: EvmAddress
    farm_id: int
    staker: EvmAddress
    amount: int

class FarmWithdraw(DomainEvent, tag=True):
    contract: EvmAddress
    farm_id: int
    staker: EvmAddress
    amount: int

class UpdateFarm(DomainEvent, tag=True):
    contract: EvmAddress
    farm_id: int
    last_reward_timestamp: int
    deposit_balance: int
    acc_reward_per_share: int

class FarmHarvest(DomainEvent, tag=True):
    contract: EvmAddress
    farm_id: int
    staker: EvmAddress
    amount_received: int
    amount_owed: int

class FarmBatchHarvest(DomainEvent, tag=True):
    contract: EvmAddress
    farm_ids: List[int]

class EmergencyWithdraw(DomainEvent, tag=True):
    contract: EvmAddress
    farm_id: int
    staker: EvmAddress
    amount: int

class FarmSkim(DomainEvent, tag=True):
    contract: EvmAddress
    token: EvmAddress
    to: EvmAddress
    amount: int