# indexer/transform/transformers/tokens/wavax.py

from typing import Tuple

from .token_base import TokenTransformer
from ....types import (
    DecodedLog,
)


class WavaxTransformer(TokenTransformer):   
    def __init__(self, contract: str):
        if not contract:
            raise ValueError("Contract address is required for WavaxTransformer")
            
        super().__init__(contract=contract)
        
        self.log_info("WavaxTransformer initialized",
                     contract_address=self.contract_address,
                     transformer_type="WAVAX_Token",
                     special_attributes="Uses src/dst/wad instead of from/to/value")

    def _get_transfer_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str]:
        """Extract WAVAX-specific transfer attributes from Transfer event"""
        try:
            if not log.attributes:
                self.log_error("Log has no attributes for WAVAX transfer extraction",
                              log_index=log.index,
                              log_name=log.name,
                              transformer_name=self.name)
                raise ValueError("Log attributes required for WAVAX transfer extraction")
            
            # WAVAX uses different attribute names than standard ERC20
            from_addr_raw = log.attributes.get("src", "")  # WAVAX-specific: src instead of from
            to_addr_raw = log.attributes.get("dst", "")    # WAVAX-specific: dst instead of to
            value_raw = log.attributes.get("wad", 0)       # WAVAX-specific: wad instead of value
            sender = ""  # WAVAX doesn't have sender in Transfer events
            
            # Convert addresses to strings with validation
            from_addr = str(from_addr_raw) if from_addr_raw is not None else ""
            to_addr = str(to_addr_raw) if to_addr_raw is not None else ""
            
            # Validate WAVAX-specific attributes are present
            if not from_addr or not to_addr:
                self.log_error("Missing required WAVAX transfer addresses",
                              log_index=log.index,
                              src=from_addr,
                              dst=to_addr,
                              available_attributes=list(log.attributes.keys()),
                              transformer_name=self.name)
                raise ValueError("Both src and dst addresses are required for WAVAX transfers")
            
            # Validate wad (amount) is present and not None
            if value_raw is None:
                self.log_error("WAVAX transfer wad is None",
                              log_index=log.index,
                              available_attributes=list(log.attributes.keys()),
                              transformer_name=self.name)
                raise ValueError("WAVAX transfer wad cannot be None")
            
            # Log warning if standard ERC20 attributes are found (might indicate wrong ABI)
            standard_attrs = ["from", "to", "value"]
            found_standard = [attr for attr in standard_attrs if attr in log.attributes]
            if found_standard:
                self.log_warning("Standard ERC20 attributes found in WAVAX transfer - check ABI",
                                log_index=log.index,
                                found_standard_attrs=found_standard,
                                wavax_attrs={"src": from_addr, "dst": to_addr, "wad": value_raw},
                                transformer_name=self.name)
            
            self.log_debug("WAVAX transfer attributes extracted successfully",
                          log_index=log.index,
                          src=from_addr,
                          dst=to_addr,
                          wad=value_raw,
                          wad_type=type(value_raw).__name__,
                          transformer_name=self.name)
            
            return from_addr, to_addr, value_raw, sender
            
        except Exception as e:
            self.log_error("Exception in WAVAX transfer attribute extraction",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          available_attributes=list(log.attributes.keys()) if log.attributes else [],
                          transformer_name=self.name)
            raise