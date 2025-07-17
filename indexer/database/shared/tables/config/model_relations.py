# indexer/database/shared/tables/config/model_relations.py

from sqlalchemy import Column, Integer, String, ForeignKey, Index
from sqlalchemy.orm import relationship

from ....base import SharedBase, SharedTimestampMixin


class DBModelContract(SharedBase, SharedTimestampMixin):
    __tablename__ = 'model_contracts'
    
    id = Column(Integer, primary_key=True)
    model_id = Column(Integer, ForeignKey('models.id', ondelete='CASCADE'), nullable=False)
    contract_id = Column(Integer, ForeignKey('contracts.id', ondelete='CASCADE'), nullable=False)
    status = Column(String(20), nullable=False, default='active')

    model = relationship("DBModel", back_populates="contracts")
    contract = relationship("DBContract", back_populates="models")
    
    __table_args__ = (
        Index('idx_model_contracts_unique', 'model_id', 'contract_id', unique=True),
        Index('idx_model_contracts_model_id', 'model_id'),
        Index('idx_model_contracts_contract_id', 'contract_id'),
    )


class DBModelToken(SharedBase, SharedTimestampMixin):
    __tablename__ = 'model_tokens'
    
    id = Column(Integer, primary_key=True)
    model_id = Column(Integer, ForeignKey('models.id', ondelete='CASCADE'), nullable=False)
    token_id = Column(Integer, ForeignKey('tokens.id', ondelete='CASCADE'), nullable=False)
    status = Column(String(20), nullable=False, default='active')

    model = relationship("DBModel", back_populates="tokens")
    token = relationship("DBToken", back_populates="models")
    
    __table_args__ = (
        Index('idx_model_tokens_unique', 'model_id', 'token_id', unique=True),
        Index('idx_model_tokens_model_id', 'model_id'),
        Index('idx_model_tokens_token_id', 'token_id'),
    )

    def __repr__(self) -> str:
        return f"<ModelToken(model_id={self.model_id}, token_id={self.token_id})>"


class DBModelSource(SharedBase, SharedTimestampMixin):
    __tablename__ = 'model_sources'
    
    id = Column(Integer, primary_key=True)
    model_id = Column(Integer, ForeignKey('models.id', ondelete='CASCADE'), nullable=False)
    source_id = Column(Integer, ForeignKey('sources.id', ondelete='CASCADE'), nullable=False)
    status = Column(String(20), nullable=False, default='active')

    model = relationship("DBModel", back_populates="sources")
    source = relationship("DBSource", back_populates="models")
    
    __table_args__ = (
        Index('idx_model_sources_unique', 'model_id', 'source_id', unique=True),
        Index('idx_model_sources_model_id', 'model_id'),
        Index('idx_model_sources_source_id', 'source_id'),
    )