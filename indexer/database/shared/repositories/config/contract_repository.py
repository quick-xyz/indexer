# indexer/database/shared/repositories/config/contract_repository.py

from typing import List, Optional, Dict
from sqlalchemy.orm import Session

from .....types import EvmAddress
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

    def to_config(self, db_contract: DBContract, abi_loader=None) -> ContractConfig:
        """Convert database contract to ContractConfig msgspec struct"""
        abi_data = None
        if abi_loader and db_contract.abi_dir and db_contract.abi_file:
            abi_data = abi_loader.load_abi(db_contract.abi_dir, db_contract.abi_file)
        
        return ContractConfig(
            address=EvmAddress(db_contract.address.address),
            status=db_contract.status,
            block_created=db_contract.block_created,
            abi_dir=db_contract.abi_dir,
            abi_file=db_contract.abi_file,
            abi=abi_data,
            transformer=db_contract.transformer,
            transform_init=db_contract.transform_init
        )

    def get_by_address_as_config(self, address: str, abi_loader=None) -> Optional[ContractConfig]:
        db_contract = self.get_by_address(address)
        if db_contract:
            return self.to_config(db_contract, abi_loader)
        return None

    def get_all_active_as_config(self, abi_loader=None) -> Dict[EvmAddress, ContractConfig]:
        db_contracts = self.get_all_active()
        return {
            EvmAddress(contract.address.address): self.to_config(contract, abi_loader)
            for contract in db_contracts
        }