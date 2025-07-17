# indexer/database/shared/tables/config/pricing.py

from typing import List
from sqlalchemy import Column, Integer, String, ForeignKey, Index, Boolean
from sqlalchemy.orm import relationship

from ....base import SharedBase, SharedTimestampMixin
from .....types import PricingConfig
from .pool import DBPool
from .address import DBAddress

class DBPricing(SharedBase, SharedTimestampMixin):
    __tablename__ = 'model_pricing'

    id = Column(Integer, primary_key=True)
    model_id = Column(Integer, ForeignKey('models.id', ondelete='CASCADE'), nullable=False)
    pool_id = Column(Integer, ForeignKey('pools.id', ondelete='CASCADE'), nullable=False)
    pricing_method = Column(String(50), nullable=False, default='global')
    price_feed = Column(Boolean, nullable=False, default=False)
    pricing_start = Column(Integer, nullable=False) 
    pricing_end = Column(Integer, nullable=True)
    status = Column(String(20), nullable=False, default='active')

    model = relationship("DBModel", back_populates="pricing")
    pool = relationship("DBPool", back_populates="pricing")

    __table_args__ = (
        Index('idx_pricing_model_id', 'model_id'),
        Index('idx_pricing_pool_id', 'pool_id'),
        Index('idx_pricing_range', 'model_id', 'pool_id', 'pricing_start', 'pricing_end'),
        Index('idx_price_feed', 'price_feed'),
        Index('idx_pricing_status', 'status'),
    )

    def __repr__(self) -> str:
        return f"<Pricing(model_id={self.model_id}, pool_id={self.pool_id}, pricing_start={self.pricing_start}, pricing_end={self.pricing_end})>"
    
    @property
    def is_active(self) -> bool:
        return self.status == 'active'
    
    @property
    def is_indefinite(self) -> bool:
        return self.pricing_end is None

    @property
    def has_direct_pricing(self) -> bool:
        return self.pricing_method in ['direct_avax', 'direct_usd']
    
    @property
    def has_global_pricing(self) -> bool:
        return self.pricing_method == 'global'
    
    @property
    def is_price_feed(self) -> bool:
        return self.price_feed

    def is_active_at_block(self, block_number: int) -> bool:
        if not self.is_active:
            return False
        
        return (self.pricing_start <= block_number and 
                (self.pricing_end is None or self.pricing_end >= block_number))
    
    def get_effective_pricing_method(self, block_number: int = None) -> str:
        if not self.pricing_start:
            return 'global'
        elif self.pricing_end and block_number < self.pricing_start:
            return 'global'
        elif self.pricing_end and block_number > self.pricing_end:
            return 'global'
        
        return self.pricing_method
    
    def validate_pool_pricing_config(self) -> List[str]:
        errors = []
        
        if self.has_direct_pricing and not self.pricing_start:
            errors.append("pricing_start required for direct pricing configuration")
        
        return errors

    @classmethod
    def from_config(cls, config: PricingConfig, model_id: int, pool_id: int) -> 'DBPricing':
        data = config.to_database_dict(model_id, pool_id)
        return cls(**data)

    @classmethod
    def get_active_config_for_pool(cls, session, model_id: int, pool_address: str, 
                                block_number: int):
        """Get active pricing configuration for a pool address at a specific block"""
        
        return session.query(cls).join(
            DBPool, cls.pool_id == DBPool.id
        ).join(
            DBAddress, DBPool.address_id == DBAddress.id
        ).filter(
            cls.model_id == model_id,
            DBAddress.address == pool_address.lower(),
            cls.status == 'active',
            cls.pricing_start <= block_number,
            (cls.pricing_end.is_(None) | (cls.pricing_end >= block_number))
        ).order_by(cls.pricing_start.desc()).first()
    
    @classmethod
    def get_price_feeds_for_model(cls, session, model_id: int, block_number: int):
        """Get all pools configured as pricing feeds for a model at a specific block"""
        return session.query(cls).filter(
            cls.model_id == model_id,
            cls.price_feed == True,
            cls.status == 'active',
            cls.pricing_start <= block_number,
            (cls.pricing_end.is_(None) | (cls.pricing_end >= block_number))
        ).all()