from msgspec import Struct
from datetime import datetime
from ...decode.model.evm import EvmAddress,EvmHash

from .base import DomainEvent

class Auction(DomainEvent, tag=True):
    lot: int
    buyer: EvmAddress
    amount_base: int
    amount_quote: int
    price: int

class AuctionDetailed(Auction, tag=True):
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