from typing import Optional
from web3 import Web3
import msgspec

from ..interfaces import LogDecoderInterface
from ..contracts.manager import ContractManager
from ..model.evm import EvmLog
from ..model.block import DecodedLog, EncodedLog
from ...utils.logger import get_logger

class LogDecoder(LogDecoderInterface):
    def __init__(self, contract_manager: ContractManager):
        self.contract_manager = contract_manager
        self.w3 = Web3()
        self.logger = get_logger(__name__)

    def build_encoded_log(self, log: EvmLog) -> EncodedLog:
        try:
            encoded_log =  EncodedLog(
                index=self.w3.to_int(hexstr=log.logIndex),
                removed=log.removed,
                contract=log.address,
                signature=log.topics[0] if log.topics else None,
                topics=log.topics,
                data=log.data
            )
            return encoded_log
        
        except Exception as e:
            self.logger.error(f"Error decoding log in tx {log['transactionHash']}: {e}")
            return None


    def decode(self, log: EvmLog) -> Optional[DecodedLog|EncodedLog]:
        if not log.address:
            return self.build_encoded_log(log)
            
        contract = self.contract_manager.get_contract(log.address)
        if not contract:
            return self.build_encoded_log(log)



        #log_dict = msgspec.json.encode(log)

        try:
            '''
            for event_abi in [abi for abi in contract.abi if abi["type"] == "event"]:
                event_name = event_abi["name"]
                try:
                    processed_log = contract.events[event_name]().process_log(log)
                    break  
                except Exception:
                    continue 
            '''
            log_dict = msgspec.json.encode(log)
            receipt = {"logs": [log_dict]}
            
            decoded_events = contract.events.process_receipt(receipt)

            if not decoded_events:
                return self.build_encoded_log(log)

            decoded_log = decoded_events[0]
            
            return DecodedLog(
                index=self.w3.to_int(hexstr=log.logIndex),
                removed=log.removed,
                contract=log.address,
                signature=log.topics[0] if log.topics else None,
                name=decoded_log["event"],
                attributes=dict(decoded_log["args"])
            )

        except Exception as e:
            self.logger.error(f"Error decoding log in tx {log.transactionHash}: {e}")
            return self.build_encoded_log(log)
        

