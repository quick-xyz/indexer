# indexer/database/shared/tables/config/label.py

from sqlalchemy import Column, Integer, String, ForeignKey, Index
from sqlalchemy.orm import relationship

from ....base import SharedBase, SharedTimestampMixin

class DBLabel(SharedBase, SharedTimestampMixin):
    __tablename__ = 'labels'

    id = Column(Integer, primary_key=True)
    address_id = Column(Integer, ForeignKey('addresses.id', ondelete='CASCADE'), 
                       nullable=False)
    value = Column(String(255), nullable=False)
    created_by = Column(String(255), nullable=False)
    type = Column(String(50), nullable=True)
    subtype = Column(String(50), nullable=True)
    status = Column(String(20), nullable=False, default='active')

    address = relationship("DBAddress", backref="labels")

    __table_args__ = (
        Index('idx_label_address_id', 'address_id'),
        Index('idx_label_created_by', 'created_by'),
        Index('idx_label_status', 'status'),
        Index('idx_label_type', 'type'),
    )

    def __repr__(self) -> str:
        return f"<Label(address_id={self.address_id}, value='{self.value}', created_by='{self.created_by}')>"
