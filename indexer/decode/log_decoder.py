# indexer/decode/log_decoder.py

from typing import Optional, Union
from web3 import Web3
import msgspec
from web3._utils.events import get_event_data

from ..contracts.manager import ContractManager
from ..types import ( 
    EncodedLog, 
    DecodedLog,
    EvmLog,
)

class LogDecoder:
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
                    
                    normalized_attributes = self._normalize_decoded_attributes(dict(event_data["args"]))
                    
                    return DecodedLog(
                        index=self.w3.to_int(hexstr=log.logIndex),
                        removed=log.removed,
                        contract=log.address.lower(),
                        signature=log.topics[0] if log.topics else None,
                        name=event_data["event"],
                        attributes=normalized_attributes 
                    )
                except Exception:
                    continue

            return self.build_encoded_log(log)

    def _normalize_decoded_attributes(self, attributes: dict) -> dict:
            normalized = {}
            
            amount_fields = {
                'value', 'amount', 'wad',  # Standard ERC20
                'amount0', 'amount1', 'amount0in', 'amount0out', 'amount1in', 'amount1out',  # DEX
                'reserve0', 'reserve1',  # Pool reserves
                'amountin', 'amountout', 'inputamount', 'outputamount',  # Aggregator
                'liquidity', 'sqrtpricex96', 'tick',  # Uniswap V3
                'amounts', 'ids'  # Liquidity Book arrays
            }
            
            for key, value in attributes.items():
                key_lower = key.lower()
                
                if (key_lower in amount_fields or 
                    any(field in key_lower for field in ['amount', 'reserve', 'liquidity', 'price', 'wad'])):
                    
                    if isinstance(value, (int, float)):
                        normalized[key] = str(value)
                    elif isinstance(value, list):
                        normalized[key] = [str(item) if isinstance(item, (int, float)) else item for item in value]
                    else:
                        normalized[key] = value
                else:
                    normalized[key] = value
            
            return normalized