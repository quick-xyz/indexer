from typing import Optional
from web3 import Web3

from ..contracts.manager import ContractManager
from ..model.evm import EvmLog
from ..model.block import DecodedLog, EncodedLog


class LogDecoder:
    def __init__(self, contract_manager: ContractManager):
        self.contract_manager = contract_manager
        self.w3 = Web3()

    def build_encoded_log(self, log: EvmLog) -> EncodedLog:
        try:
            encoded_log =  EncodedLog(
                index=self.w3.to_int(hexstr=log.logIndex),
                removed=log.removed,
                contract=log.address,
                signature=log.topics[0],
                topics=log.topics,
                data=log.data
            )
            return encoded_log
        
        except Exception as e:
            print(f"Error decoding log in tx {log['transactionHash']}: {e}")
            return None
        
        return None

    def decode(self, log: EvmLog) -> Optional[DecodedLog|EncodedLog]:
        if not log.address:
            return self.build_encoded_log(log)
            
        contract = self.contract_manager.get_contract(log.address)
        if not contract:
            return self.build_encoded_log(log)
        
        try:
            decoded_log = contract.events.process_receipt(log)

            if not decoded_log:
                return self.build_encoded_log(log)

            return DecodedLog(
                index=self.w3.to_int(hexstr=log.logIndex),
                removed=log.removed,
                contract=log.address,
                signature=log.topics[0],
                name=decoded_log["event"],
                attributes=dict(decoded_log["args"])
            )

        except Exception as e:
            print(f"Error decoding log in tx {log.transactionHash}: {e}")
            return None
        

