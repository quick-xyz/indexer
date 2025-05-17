from typing import Optional, Dict, Union
import json
import msgspec
from msgspec import Struct

from ..interfaces import ContractRegistryInterface
from ...utils.logging import setup_logger

from ...config.types import (
    ABIConfig, 
    ContractConfig,
    ContractWithABI,
)
from ...config.config_manager import config


class ContractRegistry(ContractRegistryInterface):
    """
    Registry of contracts with their ABIs.
    """
    def __init__(self, config: ConfigManager):
        self.contracts: Dict[str, ContractWithABI] = {}  # Contracts keyed by address
        self.logger = setup_logger(__name__)
        self.abi_decoder = msgspec.json.Decoder(type=ABIConfig)
        self.load_contracts()

    def load_contracts(self):
        """Load contract registry and ABIs."""

        self.logger.info(f"Loading contracts from {contracts_file}")

        try:
            with open(contracts_file) as f:
                contracts_data = json.load(f)
        except FileNotFoundError:
            self.logger.error(f"Contract registry file not found: {contracts_file}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in contract registry: {e}")
            raise
            
        contract_count = 0
        error_count = 0

        for address, data in contracts_data.items():
            address = address.lower()
            try:
                contract_config = msgspec.convert(data, type=ContractConfig)
                
                # Load the ABI file
                abi_path = abi_directory / contract_config.abi_dir / contract_config.abi
                
                self.logger.debug(f"Loading ABI for {address} ({contract_config.name}) from {abi_path}")

                try:
                    with open(abi_path) as f:
                        abi_data = self.abi_decoder.decode(f.read())
                        
                       
                    # Store contract info and ABI
                    self.contracts[address] = ContractWithABI(
                        contract_info=contract_config,
                        abi=abi_data.abi   
                    )

                    contract_count += 1
                        
                except FileNotFoundError:
                    self.logger.warning(f"No ABI file found at {abi_path}")
                    error_count += 1
                except json.JSONDecodeError as e:
                    self.logger.warning(f"Invalid JSON in ABI file for {address}: {e}")
                    error_count += 1
                    
            except msgspec.ValidationError as e:
                self.logger.warning(f"Invalid contract metadata for {address}: {e}")
                error_count += 1
        
        self.logger.info(f"Loaded {contract_count} contracts successfully. Errors: {error_count}")

    def get_contract(self, address: str) -> Optional[ContractWithABI]:
        """Get full contract info by address."""
        return self.contracts.get(address.lower())
    
    def get_abi(self, address: str) -> Optional[list]:
        """Get contract ABI by address."""
        contract = self.contracts.get(address.lower())
        return contract.abi if contract else None
    
    def has_contract(self, address: str) -> bool:
        """Check if address is a known contract."""
        return address.lower() in self.contracts