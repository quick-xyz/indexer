# indexer/decode/block_decoder.py

from web3 import Web3
from typing import Optional, Tuple, Dict

from ..contracts.manager import ContractManager
from .transaction_decoder import TransactionDecoder
from ..types import (
    Block, 
    EvmFilteredBlock, 
    EvmHash, 
    EvmTransaction,
    EvmTxReceipt,
)

class BlockDecoder:
    def __init__(self, contract_manager: ContractManager):
        self.contract_manager = contract_manager
        self.tx_decoder = TransactionDecoder(self.contract_manager)
        self.w3 = Web3()

    def merge_tx_with_receipts(self, raw_block: EvmFilteredBlock) -> Tuple[Dict[EvmHash, Tuple[EvmTransaction, EvmTxReceipt]], Optional[Dict]]:
        tx_dict = {tx.hash: tx for tx in raw_block.transactions}
        receipts_dict = {receipt.transactionHash: receipt for receipt in raw_block.receipts}

        if not tx_dict:
            error_msg = f"No valid transactions found in block {self.w3.to_int(hexstr=raw_block.block)}"
            raise ValueError(error_msg)
        if not receipts_dict:
            error_msg = f"No valid receipts found in block {self.w3.to_int(hexstr=raw_block.block)}"
            raise ValueError(error_msg)

        tx_list = set(tx_dict.keys())
        receipts_list = set(receipts_dict.keys())

        matching_hashes = tx_list & receipts_list
        merged_dict = {k: (tx_dict[k], receipts_dict[k]) for k in matching_hashes}

        if tx_list == receipts_list:
            return merged_dict, None

        diffs = {
            "tx_only": (tx_list - receipts_list),
            "receipt_only": (receipts_list - tx_list)
        }
        
        return merged_dict, diffs

    def decode_block(self, raw_block: EvmFilteredBlock) -> Block:
        """Decode a full block, including transactions and logs"""
        decoded_tx = {}
        tx_dict, diffs = self.merge_tx_with_receipts(raw_block)
        block_number = self.w3.to_int(hexstr=raw_block.block)
        timestamp = self.w3.to_int(hexstr=raw_block.timestamp)

        for tx_hash, tx_tuple in tx_dict.items():
            processed_tx = self.tx_decoder.process_tx(
                block_number, 
                timestamp, 
                tx_tuple[0], 
                tx_tuple[1]
            )
            if processed_tx:  # Only add successful transactions
                decoded_tx[tx_hash] = processed_tx

        return Block(
            block_number=block_number,
            timestamp=timestamp,
            transactions=decoded_tx
        )