# indexer/database/models/prices/block_price.py

from sqlalchemy import Column, Enum, Integer
from sqlalchemy.dialects.postgresql import NUMERIC
import enum

from ..base import BaseModel, BlockchainTimestampMixin


class NetworkType(enum.Enum):
    AVAX = "avax"
    TEST = "fugi"


class BlockPrice(BaseModel, BlockchainTimestampMixin):
    __tablename__ = 'block_prices'
    
    network = Column(Enum(NetworkType), nullable=False, index=True)
    block_number = Column(Integer, nullable=False, index=True)
    block_price = Column(NUMERIC(precision=78, scale=0), nullable=True)