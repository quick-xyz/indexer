# indexer/transform/transformers/aggregators/kyber_aggregator.py

from typing import Tuple

from .agg_base import AggregatorTransformer
from ....types import (
    DecodedLog,
    EvmAddress,
)
from ....utils.amounts import amount_to_str


class KyberAggregatorTransformer(AggregatorTransformer):    
    def __init__(self, contract: EvmAddress):
        if not contract:
            raise ValueError("Contract address is required for KyberAggregatorTransformer")
            
        super().__init__(contract=contract)
        
        self.handler_map = {
            "Swapped": self._handle_route,
        }
        
        self.log_info("KyberAggregatorTransformer initialized",
                     contract_address=self.contract_address,
                     handler_count=len(self.handler_map),
                     supported_events=list(self.handler_map.keys()))

    def _get_route_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str, str, str]:
        """Extract Kyber-specific route attributes from Swapped event"""
        try:
            if not log.attributes:
                self.log_error("Log has no attributes for Kyber route extraction",
                              log_index=log.index,
                              log_name=log.name,
                              transformer_name=self.name)
                raise ValueError("Log attributes required for Kyber route extraction")
            
            # Kyber uses different attribute names than the base class
            sender = str(log.attributes.get("sender", ""))
            to = str(log.attributes.get("dstReceiver", ""))  # Kyber-specific
            token_in = str(log.attributes.get("srcToken", ""))  # Kyber-specific
            token_out = str(log.attributes.get("dstToken", ""))  # Kyber-specific
            
            # Handle amount extraction with error checking
            try:
                amount_in = amount_to_str(log.attributes.get("spentAmount", 0))  # Kyber-specific
                amount_out = amount_to_str(log.attributes.get("returnAmount", 0))  # Kyber-specific
            except Exception as e:
                self.log_error("Failed to extract Kyber amounts from route log",
                              log_index=log.index,
                              error=str(e),
                              spent_amount_raw=log.attributes.get("spentAmount"),
                              return_amount_raw=log.attributes.get("returnAmount"),
                              transformer_name=self.name)
                raise
            
            # Validate Kyber-specific attributes are present
            if not token_in or not token_out:
                self.log_error("Missing required Kyber token attributes",
                              log_index=log.index,
                              src_token=token_in,
                              dst_token=token_out,
                              available_attributes=list(log.attributes.keys()),
                              transformer_name=self.name)
                raise ValueError("Kyber srcToken and dstToken are required")
            
            if not to:
                self.log_warning("Missing dstReceiver in Kyber swap - using empty string",
                                log_index=log.index,
                                available_attributes=list(log.attributes.keys()),
                                transformer_name=self.name)
            
            self.log_debug("Kyber route attributes extracted successfully",
                          log_index=log.index,
                          sender=sender,
                          dst_receiver=to,
                          src_token=token_in,
                          dst_token=token_out,
                          spent_amount=amount_in,
                          return_amount=amount_out,
                          transformer_name=self.name)
            
            return sender, to, token_in, token_out, amount_in, amount_out
            
        except Exception as e:
            self.log_error("Exception in Kyber route attribute extraction",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          available_attributes=list(log.attributes.keys()) if log.attributes else [],
                          transformer_name=self.name)
            raise