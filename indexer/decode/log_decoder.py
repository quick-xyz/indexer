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

        # Try to decode with each event ABI
        for event_abi in event_abis:
            try:
                event_data = get_event_data(self.w3.codec, event_abi, log_dict)
                return DecodedLog(
                    index=self.w3.to_int(hexstr=log.logIndex),
                    removed=log.removed,
                    contract=log.address.lower(),
                    signature=log.topics[0] if log.topics else None,
                    name=event_data["event"],
                    attributes=dict(event_data["args"])
                )
            except Exception:
                continue

        return self.build_encoded_log(log)
