from msgspec import Struct
from datetime import datetime
from ...decode.model.evm import EvmAddress,EvmHash

from .base import DomainEvent

class Auction(DomainEvent, tag=True):
    timestamp: datetime
    tx_hash: EvmHash
    lot: int
    buyer: EvmAddress
    amount_smol: int
    amount_avax: int
    price_avax: int
    event_tag: str = "auction"

class AuctionDetailed(DomainEvent, tag=True):
    start_price: float
    price_usd: float
    value_usd: float

class LotStarted(DomainEvent, tag=True):
    lot: int
    start_price: float
    start_time: datetime

class LotCancelled(DomainEvent, tag=True):
    lot: int
    end_price: float
    end_time: datetime

class ParamChange(DomainEvent, tag=True):
    param: str
    old_value: int
    new_value: int