# indexer/database/shared/tables/config/pool.py

from sqlalchemy import Column, Integer, String, ForeignKey, Index
from sqlalchemy.orm import relationship

from ....base import SharedBase, SharedTimestampMixin
from ....types import EvmAddressType
from .....types import PoolConfig


class DBPool(SharedBase, SharedTimestampMixin):
    __tablename__ = 'pools'
    
    id = Column(Integer, primary_key=True)
    address_id = Column(Integer, ForeignKey('addresses.id', ondelete='CASCADE'), 
                       nullable=False, unique=True)
    base_token = Column(EvmAddressType(), nullable=False)
    quote_token = Column(EvmAddressType(), nullable=True)
    pricing_default = Column(String(50), nullable=False)
    status = Column(String(50), nullable=False, default='active')
    
    address = relationship("DBAddress", backref="pool")
    pricing = relationship("DBPricing", back_populates="pool", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_pools_address_id', 'address_id'),
        Index('idx_pools_base_token', 'base_token'),
        Index('idx_pools_quote_token', 'quote_token'),
        Index('idx_pools_pricing_default', 'pricing_default'),
        Index('idx_pools_status', 'status'),
    )
    
    def __repr__(self) -> str:
        return f"<Pool(address_id={self.address_id}, base_token='{self.base_token}', pricing_default='{self.pricing_default}')>"
    
    @property
    def has_direct_pricing_default(self) -> bool:
        return self.pricing_default in ['direct_avax', 'direct_usd']
    
    @property
    def has_global_pricing_default(self) -> bool:
        return self.pricing_default == 'global'

    def get_pricing_default(self) -> str:
        return self.pricing_default

    @classmethod
    def from_config(cls, config: PoolConfig, address_id: int) -> 'DBPool':
        data = config.to_database_dict()
        data['address_id'] = address_id
        return cls(**data)