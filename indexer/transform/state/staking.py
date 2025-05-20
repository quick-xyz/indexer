from datetime import datetime
from typing import Literal, Optional
from msgspec import Struct

from ...decode.model.evm import EvmAddress,EvmHash

# TODO: STAKING SHOULD ACCOMODATED BASE STAKE, WRAPPER STAKE AND LP STAKING, AS WELL
# AS RECEIPTS AND NO RECEIPTS

class StakingPosition(Struct, tag=True):
    position_id: int #tx_hash,contract,staker,id
    contract: EvmAddress
    staker: EvmAddress
    deposit_token: EvmAddress
    deposit_amount: int
    created_at: EvmHash
    created_on: datetime
    closed_at: Optional[EvmHash] = None
    closed_on: Optional[datetime] = None
    custodian: Optional[EvmAddress] = None
    receipt_token: Optional[EvmAddress] = None
    receipt_id: Optional[int] = None
    amount_receipt: Optional[str] = None

class StakingBalance(Struct, tag=True):
    timestamp: datetime
    contract: EvmAddress
    staker: EvmAddress
    deposit_token: EvmAddress
    deposit_amount: int
    base_token: EvmAddress
    amount_base: int
    quote_token: EvmAddress
    amount_quote: int
    value_avax: Optional[int] = None
    value_usd: Optional[int] = None
    custodian: Optional[EvmAddress] = None
