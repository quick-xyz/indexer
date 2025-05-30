# indexer/types/model/liquidity.py

from typing import Literal, List, Optional

from ..new import EvmAddress
from .base import DomainEvent
from .transfer import Transfer


class Position(DomainEvent, tag=True):
    receipt_token: EvmAddress
    receipt_id: int
    amount_base: int
    amount_quote: int
    amount_receipt: Optional[int] = None
    custodian: Optional[EvmAddress] = None

class Liquidity(DomainEvent, tag=True):
    pool: EvmAddress
    provider: EvmAddress
    base_token: EvmAddress
    amount_base: int
    quote_token: EvmAddress
    amount_quote: int
    action: Literal["add_lp","remove_lp","update_lp"]
    positions: Optional[List[Position]] = None
    transfers: Optional[List[Transfer]] = None
    custodian: Optional[EvmAddress] = None

    def _get_identifying_content(self):
        return {
            "event_type": "liquidity",
            "tx_salt": self.tx_hash,
            "pool": self.pool,
            "provider": self.provider,
            "amount_base": self.amount_base,
            "amount_quote": self.amount_quote,
            "action": self.action,
        }