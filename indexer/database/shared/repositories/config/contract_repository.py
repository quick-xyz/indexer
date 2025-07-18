# indexer/database/shared/repositories/config/contract_repository.py

from typing import List, Optional
from sqlalchemy.orm import Session

from .config_base_repository import ConfigRepositoryBase
from ...tables import DBContract, DBAddress
from .....types.configs.contract import ContractConfig


class ContractRepository(ConfigRepositoryBase[DBContract, ContractConfig]):    
    def __init__(self, db_manager):
        super().__init__(db_manager, "Contract")
    
    def _get_entity_class(self) -> type:
        return DBContract
    
    def _get_by_identifier(self, session: Session, address: str) -> Optional[DBContract]:
        return session.query(DBContract).join(DBAddress).filter(
            DBAddress.address == address.lower()
        ).first()
    
    def _create_entity_from_config(self, session: Session, config: ContractConfig) -> DBContract:
        address_record = session.query(DBAddress).filter(
            DBAddress.address == config.address.lower()
        ).first()
        
        if not address_record:
            raise ValueError(f"Address {config.address} not found. Import addresses first.")
        
        # Create contract record
        contract = DBContract(
            address_id=address_record.id,
            block_created=config.block_created,
            abi_dir=config.abi_dir,
            abi_file=config.abi_file,
            transformer=config.transformer,
            transform_init=config.transform_init,
            status=config.status
        )
        
        return contract
    
    def _config_matches_entity(self, config: ContractConfig, entity: DBContract) -> bool:
        return (
            entity.block_created == config.block_created and
            entity.abi_dir == config.abi_dir and
            entity.abi_file == config.abi_file and
            entity.transformer == config.transformer and
            entity.transform_init == config.transform_init and
            entity.status == config.status
        )
    
    def _get_entity_identifier(self, config: ContractConfig) -> str:
        return f"Contract ({config.address})"
    
    def get_all_active(self) -> List[DBContract]:
        with self.db_manager.get_session() as session:
            return session.query(DBContract).filter(DBContract.status == 'active').all()
    
    def get_by_address(self, address: str) -> Optional[DBContract]:
        with self.db_manager.get_session() as session:
            return self._get_by_identifier(session, address)
    
    def get_by_transformer(self, transformer: str) -> List[DBContract]:
        with self.db_manager.get_session() as session:
            return session.query(DBContract).filter(DBContract.transformer == transformer).all()
    
    def get_with_abi(self) -> List[DBContract]:
        with self.db_manager.get_session() as session:
            return session.query(DBContract).filter(
                DBContract.abi_dir.isnot(None),
                DBContract.abi_file.isnot(None)
            ).all()