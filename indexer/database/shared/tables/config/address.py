# indexer/database/shared/tables/config/address.py

from sqlalchemy import Column, Integer, String, Text, Index

from ....base import SharedBase, SharedTimestampMixin
from ....types import EvmAddressType
from .....types import AddressConfig


class DBAddress(SharedBase, SharedTimestampMixin):
    __tablename__ = 'addresses'
    
    id = Column(Integer, primary_key=True)
    address = Column(EvmAddressType(), nullable=False, unique=True)
    name = Column(String(255), nullable=False)
    project = Column(String(255), nullable=True)
    description = Column(Text, nullable=True) 
    type = Column(String(50), nullable=False, default='contract')
    subtype = Column(String(50), nullable=True)  
    status = Column(String(50), nullable=False, default='active')
    


    __table_args__ = (
        Index('idx_addresses_address', 'address'),
        Index('idx_addresses_type', 'type'),
        Index('idx_addresses_subtype', 'subtype'),
        Index('idx_addresses_project', 'project'),
        Index('idx_addresses_status', 'status'),
    )

    def __repr__(self) -> str:
        return f"<Address(name='{self.name}', address='{self.address}', type='{self.type}')>"
    
    @property 
    def addr(self) -> str:
        return self.address
    
    @property
    def is_contract(self) -> bool:
        return self.type == 'contract'
    
    @property
    def is_eoa(self) -> bool:
        return self.type == 'eoa'
    
    @classmethod
    def from_config(cls, config: AddressConfig) -> 'DBAddress':
        data = config.to_database_dict()
        return cls(**data)