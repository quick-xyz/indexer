# indexer/database/base_repository.py

from typing import TypeVar, Generic, Type, List, Optional, Any, Dict
from sqlalchemy.orm import Session
from sqlalchemy import desc

from ..core.logging import IndexerLogger, INFO, DEBUG, WARNING, ERROR, CRITICAL
from ..types.new import EvmHash, DomainEventId


T = TypeVar('T')

class BaseRepository(Generic[T]):    
    def __init__(self, db_manager, model_class: Type[T]):
        self.db_manager = db_manager
        self.model_class = model_class
        self.logger = IndexerLogger.get_logger(f'database.repository.{model_class.__name__.lower()}')
    
    def get_by_id(self, session: Session, id: int) -> Optional[T]:
        try:
            return session.query(self.model_class).filter(self.model_class.id == id).first()
        except Exception as e:
            self.logger.error(f"Error getting {self.model_class.__name__} by ID {id}: {e}")
            raise
    
    def get_all(self, session: Session, limit: int = 100) -> List[T]:
        try:
            return session.query(self.model_class).order_by(desc(self.model_class.created_at)).limit(limit).all()
        except Exception as e:
            self.logger.error(f"Error getting all {self.model_class.__name__}: {e}")
            raise
    
    def create(self, session: Session, **kwargs) -> T:
        try:
            instance = self.model_class(**kwargs)
            session.add(instance)
            session.flush()
            
            self.logger.debug(f"Created {self.model_class.__name__} with ID: {getattr(instance, 'id', 'N/A')}")
            return instance
            
        except Exception as e:
            self.logger.error(f"Error creating {self.model_class.__name__}: {e}")
            raise
    
    def bulk_create(self, session: Session, items: List[Dict]) -> int:
        if not items:
            return 0
            
        try:
            # Use SQLAlchemy's bulk_insert_mappings for maximum performance
            session.bulk_insert_mappings(self.model_class, items)
            session.flush()
            
            count = len(items)
            self.logger.debug(f"Bulk created {count} {self.model_class.__name__} records")
            return count
            
        except Exception as e:
            self.logger.error(f"Error bulk creating {self.model_class.__name__}: {e}")
            raise
    
    def delete(self, session: Session, id: int) -> bool:
        try:
            record = self.get_by_id(session, id)
            if record:
                session.delete(record)
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error deleting {self.model_class.__name__} by ID {id}: {e}")
            raise
    
    def count(self, session: Session) -> int:
        try:
            return session.query(self.model_class).count()
        except Exception as e:
            self.logger.error(f"Error counting {self.model_class.__name__}: {e}")
            raise


class DomainEventBaseRepository(BaseRepository[T]):
    def get_by_content_id(self, session: Session, content_id: DomainEventId) -> Optional[T]:
        try:
            return session.query(self.model_class).filter(
                self.model_class.content_id == content_id
            ).first()
        except Exception as e:
            self.logger.error(f"Error getting {self.model_class.__name__} by content_id {content_id}: {e}")
            raise
    
    def get_by_tx_hash(self, session: Session, tx_hash: EvmHash) -> List[T]:
        try:
            return session.query(self.model_class).filter(
                self.model_class.tx_hash == tx_hash
            ).order_by(self.model_class.timestamp).all()
        except Exception as e:
            self.logger.error(f"Error getting {self.model_class.__name__} by tx_hash {tx_hash}: {e}")
            raise
    
    def get_by_block_range(self, session: Session, start_block: int, end_block: int) -> List[T]:
        try:
            from sqlalchemy import and_
            return session.query(self.model_class).filter(
                and_(
                    self.model_class.block_number >= start_block,
                    self.model_class.block_number <= end_block
                )
            ).order_by(self.model_class.block_number, self.model_class.timestamp).all()
        except Exception as e:
            self.logger.error(f"Error getting {self.model_class.__name__} by block range {start_block}-{end_block}: {e}")
            raise
    
    def get_recent(self, session: Session, limit: int = 100) -> List[T]:
        try:
            return session.query(self.model_class).order_by(
                desc(self.model_class.timestamp)
            ).limit(limit).all()
        except Exception as e:
            self.logger.error(f"Error getting recent {self.model_class.__name__}: {e}")
            raise
    
    def exists_by_content_id(self, session: Session, content_id: DomainEventId) -> bool:
        try:
            return session.query(
                session.query(self.model_class).filter(
                    self.model_class.content_id == content_id
                ).exists()
            ).scalar()
        except Exception as e:
            self.logger.error(f"Error checking existence of {self.model_class.__name__} by content_id {content_id}: {e}")
            raise
    
    def bulk_create_skip_existing(self, session: Session, items: List[Dict]) -> int:
        if not items:
            return 0
            
        try:
            content_ids = [item['content_id'] for item in items]
            
            existing_records = session.query(self.model_class.content_id).filter(
                self.model_class.content_id.in_(content_ids)
            ).all()
            existing_content_ids = set(record[0] for record in existing_records)
            
            new_items = [
                item for item in items 
                if item['content_id'] not in existing_content_ids
            ]
            
            if not new_items:
                self.logger.debug(f"All {len(items)} {self.model_class.__name__} records already exist, skipping")
                return 0
            
            cleaned_items = []
            for item in new_items:
                cleaned_item = {}
                for key, value in item.items():
                    if value == 'None' and key in ['token_id', 'custodian']:
                        cleaned_item[key] = None
                    else:
                        cleaned_item[key] = value
                cleaned_items.append(cleaned_item)
            
            session.bulk_insert_mappings(self.model_class, cleaned_items)
            session.flush()
            
            created_count = len(new_items)
            skipped_count = len(items) - created_count
            
            self.logger.debug(f"Bulk created {created_count} {self.model_class.__name__} records, skipped {skipped_count} existing")
            return created_count
            
        except Exception as e:
            self.logger.error(f"Error bulk creating {self.model_class.__name__} with skip existing: {e}")
            raise


class ProcessingBaseRepository(BaseRepository[T]):
    def get_by_tx_hash(self, session: Session, tx_hash: EvmHash) -> Optional[T]:
        try:
            return session.query(self.model_class).filter(
                self.model_class.tx_hash == tx_hash
            ).first()
        except Exception as e:
            self.logger.error(f"Error getting {self.model_class.__name__} by tx_hash {tx_hash}: {e}")
            raise
    
    def get_by_status(self, session: Session, status: str, limit: int = 100) -> List[T]:
        try:
            return session.query(self.model_class).filter(
                self.model_class.status == status
            ).order_by(self.model_class.created_at).limit(limit).all()
        except Exception as e:
            self.logger.error(f"Error getting {self.model_class.__name__} by status {status}: {e}")
            raise
    
    def get_by_block_number(self, session: Session, block_number: int) -> List[T]:
        try:
            return session.query(self.model_class).filter(
                self.model_class.block_number == block_number
            ).order_by(self.model_class.created_at).all()
        except Exception as e:
            self.logger.error(f"Error getting {self.model_class.__name__} by block_number {block_number}: {e}")
            raise