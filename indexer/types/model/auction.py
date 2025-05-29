from ..new import EvmAddress
from .base import DomainEvent


class AuctionPurchase(DomainEvent, tag=True):
    lot: int
    buyer: EvmAddress
    amount_base: int
    amount_quote: int
    price: int

    def _get_identifying_content(self):
        return {
            "event_type": "auction_purchase",
            "tx_salt": self.tx_hash,
            "lot": self.lot,
            "buyer": self.buyer,
            "amount_base": self.amount_base,
            "amount_quote": self.amount_quote,
            "price": self.price,
        }

class LotStarted(DomainEvent, tag=True):
    lot: int
    start_price: int
    start_time: int

    def _get_identifying_content(self):
        return {
            "event_type": "lot_started",
            "tx_salt": self.tx_hash,
            "lot": self.lot,
            "start_price": self.start_price,
            "start_time": self.start_time,
        }

class LotCancelled(DomainEvent, tag=True):
    lot: int
    end_price: int

    def _get_identifying_content(self):
        return {
            "event_type": "lot_cancelled",
            "tx_salt": self.tx_hash,
            "lot": self.lot,
            "end_price": self.end_price,
        }