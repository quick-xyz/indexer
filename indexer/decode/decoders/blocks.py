from web3 import Web3
import datetime
from typing import Optional

from ..contracts.registry import ContractRegistry
from ..contracts.manager import ContractManager
from .transaction import TransactionDecoder
from ..model.block import Block
from ..model.evm import EvmFilteredBlock,EvmHash,EvmTransaction,EvmTxReceipt
from ..utils.logging import setup_logger

def hex_timestamp_to_datetime(w3: Web3,hex_timestamp):
    try:
        unix_timestamp = w3.to_int(hexstr=hex_timestamp)
        return datetime.datetime.fromtimestamp(unix_timestamp)
    except ValueError:
        return "Invalid hexadecimal timestamp"


class BlockDecoder:
    def __init__(self, registry: ContractRegistry):
        self.contract_manager = ContractManager(registry)
        self.tx_decoder = TransactionDecoder(self.contract_manager)
        self.w3 = Web3()
        self.logger = setup_logger(__name__)

    def merge_tx_with_receipts(self, raw_block: EvmFilteredBlock) -> tuple[dict[EvmHash,tuple[EvmTransaction,EvmTxReceipt]],Optional[dict]]:
        tx_dict = {tx.hash: tx for tx in raw_block.transactions}
        receipts_dict = {receipt.transactionHash: receipt for receipt in raw_block.receipts}

        if not tx_dict:
            error_msg = f"No valid transactions found in block {self.w3.to_int(hexstr=raw_block.block)}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        if not receipts_dict:
            error_msg = f"No valid receipts found in block {self.w3.to_int(hexstr=raw_block.block)}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        self.logger.info(f"Processing block: {self.w3.to_int(hexstr=raw_block.block)} with {len(tx_dict)} transactions and {len(receipts_dict)} receipts")

        tx_list = set(tx_dict.keys())
        receipts_list = set(receipts_dict.keys())

        matching_hashes = tx_list & receipts_list
        self.logger.debug(f"Found {len(matching_hashes)} matching transactions and receipts")

        merged_dict = {k: (tx_dict[k], receipts_dict[k]) for k in matching_hashes}

        if tx_list == receipts_list:
            return merged_dict, None

        diffs = {
            "tx_only": (tx_list - receipts_list),
            "receipt_only": (receipts_list - tx_list)
        }     
           
        if diffs["tx_only"]:
            self.logger.warning(f"Transactions without receipts: {len(diffs['tx_only'])}")
            for tx_hash in diffs["tx_only"][:5]:  # Log first 5 for brevity
                self.logger.debug(f"TX without receipt: {tx_hash}")
                
        if diffs["receipt_only"]:
            self.logger.warning(f"Receipts without transactions: {len(diffs['receipt_only'])}")
            for receipt_hash in diffs["receipt_only"][:5]:  # Log first 5 for brevity
                self.logger.debug(f"Receipt without TX: {receipt_hash}")
                
        return merged_dict,diffs

    def decode_block(self, raw_block: EvmFilteredBlock) -> Block:
        """
        Decode a full block, including transactions and logs.
        """
        decoded_tx = {}
        tx_dict, diffs = self.merge_tx_with_receipts(raw_block)

        if diffs:
            #add handling for diffs
            pass

        for tx_hash,tx_tuple in tx_dict.items():
            # pass tx_tuple to the transaction processor, return decoded tx object
            processed_tx = self.tx_decoder.process_tx(tx_tuple[0],tx_tuple[1])
            decoded_tx[tx_hash] = processed_tx

        return Block(
            block_number=self.w3.to_int(raw_block.block),
            timestamp=hex_timestamp_to_datetime(self.w3,raw_block.timestamp),
            transactions=decoded_tx
        )