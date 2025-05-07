# In models/gcs.py (new file)
from datetime import datetime
from sqlalchemy import Column, String, BigInteger, DateTime
from ..models.base import Base

class GcsObject(Base):
    __tablename__ = "gcs_objects"
    
    path = Column(String, primary_key=True)
    block_number = Column(BigInteger, nullable=True, index=True)
    file_type = Column(String(20), nullable=False)  # 'raw', 'decoded'
    size = Column(BigInteger)
    updated_at = Column(DateTime, nullable=False, default=datetime.now)
    
    def __repr__(self):
        return f"<GcsObject(path={self.path}, block_number={self.block_number}, type={self.file_type})>"