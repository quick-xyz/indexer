# indexer/transform/transformers/aggregators/odos_aggregator.py

from typing import Tuple

from .agg_base import AggregatorTransformer
from ....types import (
    DecodedLog,
    EvmAddress,
)
from ....utils.amounts import amount_to_str


class OdosAggregatorTransformer(AggregatorTransformer):    
    def __init__(self, contract: EvmAddress):
        if not contract:
            raise ValueError("Contract address is required for OdosAggregatorTransformer")
            
        super().__init__(contract=contract)
        
        self.handler_map = {
            "Swap": self._handle_route,
            "SwapMulti": self._handle_multi_route,
        }
        
        self.log_info("OdosAggregatorTransformer initialized",
                     contract_address=self.contract_address,
                     handler_count=len(self.handler_map),
                     supported_events=list(self.handler_map.keys()),
                     transformer_type="ODOS_Aggregator")
    
    def _get_route_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str, str, str]:
        """Extract ODOS-specific route attributes from Swap event"""
        try:
            if not log.attributes:
                self.log_error("Log has no attributes for ODOS route extraction",
                              log_index=log.index,
                              log_name=log.name,
                              transformer_name=self.name)
                raise ValueError("Log attributes required for ODOS route extraction")
            
            # ODOS uses different attribute names than the base class
            sender = str(log.attributes.get("sender", ""))
            to = str(log.attributes.get("to", ""))
            token_in = str(log.attributes.get("inputToken", ""))  # ODOS-specific
            token_out = str(log.attributes.get("outputToken", ""))  # ODOS-specific
            
            # Handle amount extraction with error checking
            try:
                amount_in = amount_to_str(log.attributes.get("inputAmount", 0))  # ODOS-specific
                amount_out = amount_to_str(log.attributes.get("amountOut", 0))  # ODOS-specific
            except Exception as e:
                self.log_error("Failed to extract ODOS amounts from route log",
                              log_index=log.index,
                              error=str(e),
                              input_amount_raw=log.attributes.get("inputAmount"),
                              amount_out_raw=log.attributes.get("amountOut"),
                              transformer_name=self.name)
                raise
            
            # Validate ODOS-specific attributes are present
            if not token_in or not token_out:
                self.log_error("Missing required ODOS token attributes",
                              log_index=log.index,
                              input_token=token_in,
                              output_token=token_out,
                              available_attributes=list(log.attributes.keys()),
                              transformer_name=self.name)
                raise ValueError("ODOS inputToken and outputToken are required")
            
            if not to:
                self.log_warning("Missing 'to' address in ODOS swap",
                                log_index=log.index,
                                available_attributes=list(log.attributes.keys()),
                                transformer_name=self.name)
            
            if not sender:
                self.log_warning("Missing 'sender' address in ODOS swap",
                                log_index=log.index,
                                available_attributes=list(log.attributes.keys()),
                                transformer_name=self.name)
            
            self.log_debug("ODOS route attributes extracted successfully",
                          log_index=log.index,
                          sender=sender,
                          to=to,
                          input_token=token_in,
                          output_token=token_out,
                          input_amount=amount_in,
                          amount_out=amount_out,
                          transformer_name=self.name)
            
            return sender, to, token_in, token_out, amount_in, amount_out
            
        except Exception as e:
            self.log_error("Exception in ODOS route attribute extraction",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          available_attributes=list(log.attributes.keys()) if log.attributes else [],
                          transformer_name=self.name)
            raise