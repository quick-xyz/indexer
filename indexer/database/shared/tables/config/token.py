# indexer/database/shared/tables/config/token.py

from sqlalchemy import Column, Integer, String, ForeignKey, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from ....base import SharedBase, SharedTimestampMixin
from .....types import TokenConfig


class DBToken(SharedBase, SharedTimestampMixin):
    __tablename__ = 'tokens'
    
    id = Column(Integer, primary_key=True)
    address_id = Column(Integer, ForeignKey('addresses.id', ondelete='CASCADE'), 
                       nullable=False, unique=True)
    symbol = Column(String(50), nullable=False)
    decimals = Column(Integer, nullable=False, default=18)
    token_type = Column(String(20), nullable=False, default='erc20')
    status = Column(String(50), nullable=False, default='active')

    address = relationship("DBAddress", backref="token")
    models = relationship("DBModelToken", back_populates="token")

    __table_args__ = (
        Index('idx_tokens_address_id', 'address_id'),
        Index('idx_tokens_symbol', 'symbol'),
        Index('idx_tokens_type', 'token_type'), 
        Index('idx_tokens_status', 'status'),
    )

    def __repr__(self) -> str:
        return f"<Token(symbol='{self.symbol}', decimals={self.decimals}, address_id={self.address_id})>"
    
    @property
    def human_readable_amount(self) -> callable:
        """Get function to convert raw amounts to human-readable format"""
        def convert(raw_amount: int) -> float:
            return raw_amount / (10 ** self.decimals)
        return convert
    
    @property
    def raw_amount(self) -> callable:
        """Get function to convert human-readable amounts to raw format"""
        def convert(human_amount: float) -> int:
            return int(human_amount * (10 ** self.decimals))
        return convert
    
    @classmethod
    def from_config(cls, config: TokenConfig, address_id: int) -> 'DBToken':
        """Create Token from validated TokenConfig"""
        data = config.to_database_dict()
        data['address_id'] = address_id
        return cls(**data)

    @property
    def is_nft(self) -> bool:
        return self.token_type in ['erc721', 'erc1155']
    
    @property
    def is_fungible(self) -> bool:
        return self.token_type == 'erc20'