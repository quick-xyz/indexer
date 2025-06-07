# indexer/types/model/auction.py

from ..new import EvmAddress, EvmHash
from .base import DomainEvent, Signal


class AuctionPurchaseSignal(Signal, tag=True):
    lot: int
    buyer: EvmAddress
    base_amount: str
    quote_amount: str
    price: str

class LotStartSignal(Signal, tag=True):
    lot: int
    start_price: str
    start_time: int

class LotCancelSignal(Signal, tag=True):
    lot: int
    end_price: str

class AuctionPurchase(DomainEvent, tag=True, kw_only=True):
    lot: int
    buyer: EvmAddress
    base_amount: str
    quote_amount: str
    price: str

    @classmethod
    def from_signal(cls, signal: AuctionPurchaseSignal, timestamp: int, tx_hash: EvmHash):
        return cls(
            timestamp=timestamp,
            tx_hash=tx_hash,
            lot=signal.lot,
            buyer=signal.buyer,
            base_amount=signal.base_amount,
            quote_amount=signal.quote_amount,
            price=signal.price,
        )
    
    def _get_identifying_content(self):
        return {
            "event_type": "auction_purchase",
            "tx_salt": self.tx_hash,
            "lot": self.lot,
            "buyer": self.buyer,
            "base_amount": self.base_amount,
            "quote_amount": self.quote_amount,
            "price": self.price,
        }

class LotStarted(DomainEvent, tag=True, kw_only=True):
    lot: int
    start_price: str
    start_time: int

    def from_signal(cls, signal: LotStartSignal, timestamp: int, tx_hash: EvmHash):
        return cls(
            timestamp=timestamp,
            tx_hash=tx_hash,
            lot=signal.lot,
            start_price=signal.start_price,
            start_time=signal.start_time,
        )
    
    def _get_identifying_content(self):
        return {
            "event_type": "lot_started",
            "tx_salt": self.tx_hash,
            "lot": self.lot,
            "start_price": self.start_price,
            "start_time": self.start_time,
        }

class LotCancelled(DomainEvent, tag=True, kw_only=True):
    lot: int
    end_price: str

    def from_signal(cls, signal: LotCancelSignal, timestamp: int, tx_hash: EvmHash):
        return cls(
            timestamp=timestamp,
            tx_hash=tx_hash,
            lot=signal.lot,
            end_price=signal.end_price,
        )
    
    def _get_identifying_content(self):
        return {
            "event_type": "lot_cancelled",
            "tx_salt": self.tx_hash,
            "lot": self.lot,
            "end_price": self.end_price,
        }