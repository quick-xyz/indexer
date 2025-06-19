# indexer/transform/transformers/pools/phar_pair.py

from typing import Tuple

from .pool_base import PoolTransformer
from ....types import DecodedLog, EvmAddress
from ....utils.amounts import amount_to_str

class PharPairTransformer(PoolTransformer):
    def __init__(self, contract: EvmAddress, token0: EvmAddress, token1: EvmAddress, base_token: EvmAddress):
        if not contract or not token0 or not token1 or not base_token:
            raise ValueError("All addresses are required for PharPairTransformer")
            
        super().__init__(contract, token0, token1, base_token)
        
        self.log_info("PharPairTransformer initialized",
                     contract_address=self.contract_address,
                     token0=self.token0,
                     token1=self.token1,
                     base_token=self.base_token,
                     transformer_type="Pharaoh_Pair",
                     special_behavior="Uses 'amount' instead of 'value' for transfers")

    def _get_transfer_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str]:
        """Extract Pharaoh-specific transfer attributes from Transfer event"""
        try:
            if not log.attributes:
                self.log_error("Log has no attributes for Pharaoh transfer extraction",
                              log_index=log.index,
                              log_name=log.name,
                              transformer_name=self.name)
                raise ValueError("Log attributes required for Pharaoh transfer extraction")
            
            from_addr = str(log.attributes.get("from", ""))
            to_addr = str(log.attributes.get("to", ""))
            sender = str(log.attributes.get("sender", ""))
            
            # Pharaoh uses 'amount' instead of 'value' for transfers
            amount_raw = log.attributes.get("amount", 0)
            
            # Extract value with error handling
            try:
                value = amount_to_str(amount_raw)
            except Exception as e:
                self.log_error("Failed to extract amount from Pharaoh transfer event",
                              log_index=log.index,
                              error=str(e),
                              amount_raw=amount_raw,
                              transformer_name=self.name)
                raise
            
            # Validate required addresses
            if not from_addr or not to_addr:
                self.log_error("Missing required addresses in Pharaoh transfer event",
                              log_index=log.index,
                              from_addr=from_addr,
                              to_addr=to_addr,
                              available_attributes=list(log.attributes.keys()),
                              transformer_name=self.name)
                raise ValueError("Both from and to addresses are required for Pharaoh transfer events")
            
            # Log warning if standard 'value' attribute is found (might indicate wrong ABI)
            if "value" in log.attributes:
                self.log_warning("Standard 'value' attribute found in Pharaoh transfer - check ABI",
                                log_index=log.index,
                                standard_value=log.attributes.get("value"),
                                pharaoh_amount=amount_raw,
                                transformer_name=self.name)
            
            self.log_debug("Pharaoh transfer attributes extracted successfully",
                          log_index=log.index,
                          from_addr=from_addr,
                          to_addr=to_addr,
                          amount=value,
                          amount_raw=amount_raw,
                          sender=sender,
                          transformer_name=self.name)
            
            return from_addr, to_addr, value, sender
            
        except Exception as e:
            self.log_error("Exception in Pharaoh transfer attribute extraction",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          available_attributes=list(log.attributes.keys()) if log.attributes else [],
                          transformer_name=self.name)
            raise