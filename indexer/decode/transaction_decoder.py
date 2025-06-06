# indexer/decode/transaction_decoder.py

from typing import Optional, Dict, Union
from web3 import Web3

from ..contracts.manager import ContractManager
from .log_decoder import LogDecoder
from ..types import (
    EncodedMethod, 
    DecodedMethod, 
    EncodedLog, 
    DecodedLog,
    EvmTransaction,
    EvmTxReceipt,
    Transaction,
)
from ..utils.amounts import amount_to_str

def hex_to_bool(hex_string: str) -> bool:
    """Convert hex string to boolean"""
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

    def decode_function(self, tx: EvmTransaction) -> Union[EncodedMethod, DecodedMethod]:
        if not tx.to:
            return EncodedMethod(data=tx.input)

        contract = self.contract_manager.get_contract(tx.to)
        if not contract or not tx.input or tx.input == '0x':
            return EncodedMethod(data=tx.input)

        try:
            func_obj, func_params = contract.decode_function_input(tx.input)
            return DecodedMethod(
                selector=tx.input[:10],
                name=func_obj.fn_name,
                args=dict(func_params),
            )
        except Exception:
            return EncodedMethod(data=tx.input)

    def decode_receipt(self, receipt: EvmTxReceipt) -> Dict[int, Union[EncodedLog, DecodedLog]]:
        logs = {}
        for log in receipt.logs:
            index = self.w3.to_int(hexstr=log.logIndex)
            processed_log = self.log_decoder.decode(log)
            if processed_log:  # Only add successful logs
                logs[index] = processed_log
        return logs

    def process_tx(self, block_number: int, timestamp: int, tx: EvmTransaction, receipt: EvmTxReceipt) -> Optional[Transaction]:
        try:
            tx_function = self.decode_function(tx)
            tx_logs = self.decode_receipt(receipt)
            tx_value = self.w3.to_int(hexstr=tx.value) if tx.value else 0

            return Transaction(
                block=block_number,
                timestamp=timestamp,
                tx_hash=tx.hash,
                index=self.w3.to_int(hexstr=tx.transactionIndex),
                origin_from=tx.from_,
                origin_to=tx.to,
                function=tx_function,
                value=amount_to_str(tx_value),
                tx_success=hex_to_bool(receipt.status),
                logs=tx_logs
            )

        except Exception as e:
            # Could add logging back later
            return None