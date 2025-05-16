from web3 import Web3
from datetime import datetime
from typing import List, Dict, Union, Optional, Any
import json

class QuickNodeRPCClient:
    """
    A client for interacting with Ethereum blockchain via QuickNode RPC.
    """
    
    def __init__(self, endpoint_url: str):
        """
        Initialize the QuickNode RPC client.
        
        Args:
            endpoint_url (str): The QuickNode RPC endpoint URL
        """
        self.endpoint_url = endpoint_url
        self.w3 = Web3(Web3.HTTPProvider(endpoint_url))
        
        if not self.w3.is_connected():
            raise ConnectionError("Failed to connect to QuickNode RPC endpoint")
    
    def get_latest_block_number(self) -> int:
        """Get the latest block number on the blockchain."""
        return self.w3.eth.block_number
    
    def get_block(self, block_identifier: Union[int, str], full_transactions: bool = False) -> Dict:
        """
        Get block details by number or hash.
        
        Args:
            block_identifier: Block number (int) or block hash (str) or 'latest', 'earliest', 'pending', 'finalized', 'safe'
            full_transactions: Whether to include full transaction objects or just hashes
            
        Returns:
            Dict containing block details
        """
        block = self.w3.eth.get_block(block_identifier, full_transactions=full_transactions)
        return dict(block)
    
    def get_block_formatted(self, block_identifier: Union[int, str], full_transactions: bool = False) -> Dict:
        """Get block with formatted data for easier reading."""
        block = self.get_block(block_identifier, full_transactions)
        
        # Format timestamp
        if 'timestamp' in block:
            block['timestamp_formatted'] = datetime.fromtimestamp(
                block['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
        
        # Format hashes
        for key in ['hash', 'parentHash', 'sha3Uncles', 'stateRoot', 'transactionsRoot', 'receiptsRoot']:
            if key in block and block[key]:
                block[f"{key}_hex"] = block[key].hex()
        
        return block
    
    def get_blocks_range(self, start_block: int, end_block: int, full_transactions: bool = False) -> List[Dict]:
        """
        Get a range of blocks.
        
        Args:
            start_block: Starting block number (inclusive)
            end_block: Ending block number (inclusive)
            full_transactions: Whether to include full transaction objects
            
        Returns:
            List of block objects
        """
        blocks = []
        for block_num in range(start_block, end_block + 1):
            block = self.get_block(block_num, full_transactions)
            blocks.append(block)
        return blocks
    
    def get_transaction_count(self, block_identifier: Union[int, str]) -> int:
        """Get the number of transactions in a block."""
        return self.w3.eth.get_block_transaction_count(block_identifier)
    
    def get_uncle_count(self, block_identifier: Union[int, str]) -> int:
        """Get the number of uncles in a block."""
        return self.w3.eth.get_uncle_count(block_identifier)
    
    def get_block_receipts(self, block_number: int) -> List[Dict]:
        """
        Get all transaction receipts for a block.
        Note: This is a non-standard RPC method that might only be available on certain QuickNode plans.
        
        Args:
            block_number: Block number
            
        Returns:
            List of transaction receipt objects
        """
        try:
            response = self.w3.provider.make_request("eth_getBlockReceipts", [hex(block_number)])
            if 'result' in response:
                return response['result']
            else:
                raise ValueError(f"Error in response: {response}")
        except Exception as e:
            raise Exception(f"Failed to get block receipts. The endpoint might not support this method: {e}")
    
    def find_address_transactions(self, block_identifier: Union[int, str], address: str) -> List[Dict]:
        """
        Find transactions involving a specific address in a block.
        
        Args:
            block_identifier: Block number or hash
            address: Ethereum address to look for
            
        Returns:
            List of transactions involving the address
        """
        address = address.lower()
        block = self.get_block(block_identifier, full_transactions=True)
        
        matching_txs = []
        for tx in block['transactions']:
            tx_dict = dict(tx) if not isinstance(tx, dict) else tx
            
            from_address = tx_dict.get('from', '').lower()
            to_address = tx_dict.get('to', '').lower() if tx_dict.get('to') else None
            
            if from_address == address or to_address == address:
                # Format values for better readability
                if 'value' in tx_dict:
                    tx_dict['value_eth'] = self.w3.from_wei(tx_dict['value'], 'ether')
                
                # Format hashes
                if 'hash' in tx_dict and hasattr(tx_dict['hash'], 'hex'):
                    tx_dict['hash_hex'] = tx_dict['hash'].hex()
                
                matching_txs.append(tx_dict)
        
        return matching_txs
    
    def get_finalized_block(self) -> Dict:
        """
        Get the latest finalized block (post-merge Ethereum).
        
        Returns:
            Dict containing finalized block details
        """
        try:
            return self.get_block('finalized')
        except Exception as e:
            raise Exception(f"Failed to get finalized block. This might not be supported: {e}")
    
    def get_finalization_status(self) -> Dict:
        """
        Get information about block finalization status.
        
        Returns:
            Dict with information about latest, safe, and finalized blocks
        """
        try:
            latest = self.get_block('latest')
            finalized = self.get_block('finalized')
            safe = self.get_block('safe')
            
            return {
                'latest_block': latest['number'],
                'safe_block': safe['number'],
                'finalized_block': finalized['number'],
                'blocks_until_safe': latest['number'] - safe['number'],
                'blocks_until_finalized': latest['number'] - finalized['number'],
            }
        except Exception as e:
            raise Exception(f"Failed to get finalization status: {e}")
    
    def get_gas_information(self, block_identifier: Union[int, str] = 'latest') -> Dict:
        """
        Get gas information from a block.
        
        Args:
            block_identifier: Block identifier
            
        Returns:
            Dict with gas information
        """
        block = self.get_block(block_identifier)
        
        return {
            'gas_used': block['gasUsed'],
            'gas_limit': block['gasLimit'],
            'usage_percentage': (block['gasUsed'] / block['gasLimit']) * 100,
            'base_fee_per_gas': block.get('baseFeePerGas', 0),
            'base_fee_per_gas_gwei': self.w3.from_wei(block.get('baseFeePerGas', 0), 'gwei')
        }
    
    def make_custom_request(self, method: str, params: List) -> Any:
        """
        Make a custom RPC request to the QuickNode endpoint.
        
        Args:
            method: RPC method name
            params: List of parameters for the method
            
        Returns:
            The response from the RPC endpoint
        """
        return self.w3.provider.make_request(method, params)