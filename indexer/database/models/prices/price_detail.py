

class EventPrice(DomainEventModel):
    __tablename__ = 'event_prices'
    
    content_id = Column(Integer, nullable=False, index=True)
    denom = Column(EvmAddressType(), nullable=False, index=True)
    value = Column(NUMERIC(precision=78, scale=0), nullable=False)
    
    def __repr__(self) -> str:
        return f"<Liquidity(pool={self.pool[:10]}..., provider={self.provider[:10]}..., {self.action.value})>"