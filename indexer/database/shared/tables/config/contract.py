# indexer/database/shared/tables/config/contract.py

from typing import Optional
from sqlalchemy import Column, Integer, String, ForeignKey, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from ....base import SharedBase, SharedTimestampMixin
from .....types import ContractConfig


class DBContract(SharedBase, SharedTimestampMixin):
    __tablename__ = 'contracts'
    
    id = Column(Integer, primary_key=True)
    address_id = Column(Integer, ForeignKey('addresses.id', ondelete='CASCADE'), 
                       nullable=False, unique=True)
    block_created = Column(Integer, nullable=True)
    abi_dir = Column(String(255), nullable=True) 
    abi_file = Column(String(255), nullable=True) 
    transformer = Column(String(255), nullable=True)      
    transform_init = Column(JSONB, nullable=True)         
    status = Column(String(50), nullable=False, default='active') 

    address = relationship("DBAddress", backref="contract")
    models = relationship("DBModelContract", back_populates="contract")

    __table_args__ = (
        Index('idx_contracts_address_id', 'address_id'),
        Index('idx_contracts_block_created', 'block_created'),
        Index('idx_contracts_transformer', 'transformer'),
        Index('idx_contracts_status', 'status'),
    )

    def __repr__(self) -> str:
        return f"<Contract(address_id={self.address_id}, transformer='{self.transformer}')>"
    
    @property
    def has_abi_config(self) -> bool:
        return self.abi_dir is not None and self.abi_file is not None
    
    @property
    def has_transformer(self) -> bool:
        return self.transformer is not None
    
    @property
    def abi_path(self) -> Optional[str]:
        if self.has_abi_config:
            return f"{self.abi_dir}/{self.abi_file}"
        return None
    
    @classmethod
    def from_config(cls, config: ContractConfig, address_id: int) -> 'DBContract':
        data = config.to_database_dict()
        data['address_id'] = address_id
        return cls(**data)