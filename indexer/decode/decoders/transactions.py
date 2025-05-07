from typing import Optional
from web3 import Web3

from ..contracts.manager import ContractManager
from ..model.evm import EvmTransaction, EvmTxReceipt
from ..model.block import DecodedLog, EncodedLog, EncodedMethod, DecodedMethod, Transaction
from .log import LogDecoder


def hex_to_bool(hex_string):
    """Converts a hex string to a boolean."""
    if hex_string == '0x0':
        return False
    elif hex_string == '0x1':
        return True
    else:
        raise ValueError("Invalid hex string for boolean conversion")


class TransactionDecoder:
    def __init__(self, contract_manager: ContractManager):
        self.contract_manager = contract_manager
        self.log_decoder = LogDecoder(contract_manager)
        self.w3 = Web3()

    def decode_function(self, tx: EvmTransaction) -> EncodedMethod|DecodedMethod:
        if not tx.to:
            return EncodedMethod(tx.input)

        contract = self.contract_manager.get_contract(tx.to)
        if not contract or not tx.input or tx.input == '0x':
            return EncodedMethod(tx.input)

        try:
            func_obj, func_params = contract.decode_function_input(tx.input)
            
            return DecodedMethod(
                selector = func_obj.selector.hex(),
                name= func_obj.fn_name,
                args = dict(func_params),
            )
        except:
            return EncodedMethod(tx.input)

    def decode_receipt(self, receipt: EvmTxReceipt) -> dict[str,EncodedLog|DecodedLog]:
        logs = {}
        for log in receipt.logs:
            hash = log.transactionHash
            index = self.w3.to_int(hexstr=log.logIndex)
            log_id = str(hash) + str(index)
            processed_log = self.log_decoder.decode(log)
            logs[log_id] = processed_log
        return logs
    
    def process_tx(self, tx: EvmTransaction, receipt: EvmTxReceipt) -> Optional[Transaction]:
        try:
            tx_function = self.decode_function(tx)
            tx_logs = self.decode_receipt(receipt)

            return Transaction(
                tx_hash = tx.hash,
                index = self.w3.to_int(hexstr=tx.transactionIndex),
                origin_from = tx.from_,
                origin_to = tx.to,
                function = tx_function,
                tx_success = hex_to_bool(receipt.status),
                logs = tx_logs
            )

        except Exception as e:
            print(f"Error decoding transaction {tx.hash}: {e}")
            return None
