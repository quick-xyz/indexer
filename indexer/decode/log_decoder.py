# indexer/decode/log_decoder.py

from typing import Optional, Union
from web3 import Web3
import msgspec
from web3._utils.events import get_event_data
from hexbytes import HexBytes
from eth_utils import is_bytes, is_hex

from ..contracts.manager import ContractManager
from ..types import ( 
    EncodedLog, 
    DecodedLog,
    EvmLog,
)
from ..core.logging import LoggingMixin, INFO, DEBUG, WARNING, ERROR, CRITICAL

class LogDecoder(LoggingMixin):
    def __init__(self, contract_manager: ContractManager):
        self.contract_manager = contract_manager
        self.w3 = Web3()

    def build_encoded_log(self, log: EvmLog) -> Optional[EncodedLog]:
        try:
            return EncodedLog(
                index=self.w3.to_int(hexstr=log.logIndex),
                removed=log.removed,
                contract=log.address,
                signature=log.topics[0] if log.topics else None,
                topics=log.topics,
                data=log.data
            )
        except Exception:
            return None

    def decode(self, log: EvmLog) -> Optional[Union[DecodedLog, EncodedLog]]:
        if not log.address:
            return self.build_encoded_log(log)
            
        contract = self.contract_manager.get_contract(log.address)
        if not contract:
            return self.build_encoded_log(log)

        event_abis = [abi for abi in contract.abi if abi["type"] == "event"]
        log_dict = msgspec.structs.asdict(log)

        for event_abi in event_abis:
            try:
                event_data = get_event_data(self.w3.codec, event_abi, log_dict)
                
                # Handle specific case: convert bytes in 'amounts' field to hex strings
                args = dict(event_data["args"])
                
                if 'amounts' in args and isinstance(args['amounts'], list):
                    self.log_debug("Processing amounts field",
                                log_index=self.w3.to_int(hexstr=log.logIndex),
                                event_name=event_data["event"],
                                amounts_count=len(args['amounts']),
                                first_amount_type=type(args['amounts'][0]).__name__ if args['amounts'] else None)
                    
                    converted_amounts = []
                    for i, item in enumerate(args['amounts']):
                        if isinstance(item, (bytes, HexBytes)):
                            hex_value = item.hex()
                            converted_amounts.append(hex_value)
                            self.log_debug("Converted bytes to hex",
                                        log_index=self.w3.to_int(hexstr=log.logIndex),
                                        amount_index=i,
                                        original_type=type(item).__name__,
                                        hex_length=len(hex_value))
                        else:
                            converted_amounts.append(item)
                            self.log_debug("Amount already correct type",
                                        log_index=self.w3.to_int(hexstr=log.logIndex),
                                        amount_index=i,
                                        item_type=type(item).__name__)
                    
                    args['amounts'] = converted_amounts
                    self.log_debug("Amounts field processing completed",
                                log_index=self.w3.to_int(hexstr=log.logIndex),
                                final_count=len(converted_amounts))
                
                return DecodedLog(
                    index=self.w3.to_int(hexstr=log.logIndex),
                    removed=log.removed,
                    contract=log.address.lower(),
                    signature=log.topics[0] if log.topics else None,
                    name=event_data["event"],
                    attributes=args,
                )
            except Exception:
                continue

        return self.build_encoded_log(log)

    def _normalize_decoded_attributes(self, attributes: dict) -> dict:
        normalized = {}

        for key, value in attributes.items():
            if isinstance(value, list):
                normalized[key] = [self._convert_web3_attribute(item) for item in value]
            else:
                normalized[key] = self._convert_web3_attribute(value)
                    
        return normalized
    
    def _convert_web3_attribute(self, value):
        try:
            if isinstance(value, bool):
                return value
            elif isinstance(value, (bytes,HexBytes)):
                return value.hex()
            elif is_bytes(value):
                return value.hex()
            elif isinstance(value, str) and (value.startswith('0x') or is_hex(value)):
                return value
            else:
                return str(value)
        except Exception as e:
            self.log_error("Error converting web3 attribute", 
                        value=value, 
                        error=str(e), 
                        exception_type=type(e).__name__)
            return str(value)
