# indexer/types/configs/token.py

from typing import Dict, Any
from msgspec import Struct
from ..new import EvmAddress


class TokenConfig(Struct):
    address: EvmAddress
    symbol: str
    decimals: int = 18
    token_type: str = 'erc20'
    status: str = 'active'

    def validate(self):
        if self.token_type in ['erc721', 'erc1155']:
            if self.decimals != 0:
                raise ValueError(f"NFT tokens must have decimals=0, got {self.decimals}")
        elif self.token_type == 'erc20':
            if self.decimals < 0 or self.decimals > 77:  # Reasonable bounds
                raise ValueError(f"ERC20 decimals must be 0-77, got {self.decimals}")