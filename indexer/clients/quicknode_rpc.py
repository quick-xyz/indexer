# indexer/clients/quicknode_rpc.py

from datetime import datetime
from typing import List, Dict, Union, Any, Optional
from decimal import Decimal
from web3 import Web3


class QuickNodeRpcClient:
    """
    A client for interacting with Ethereum blockchain via QuickNode RPC.
    """
    
    # Chainlink AVAX/USD price feed on Avalanche mainnet
    CHAINLINK_AVAX_USD_FEED = "0x0A77230d17318075983913bC2145DB16C7366156"
    
    def __init__(self, endpoint_url: str):
        self.endpoint_url = endpoint_url
        self.w3 = Web3(Web3.HTTPProvider(endpoint_url))
        
        if not self.w3.is_connected():
            raise ConnectionError("Failed to connect to QuickNode RPC endpoint")
    
    def get_latest_block_number(self) -> int:
        return self.w3.eth.block_number
    
    def get_block(self, block_number: int, full_transactions: bool = True) -> Dict[str, Any]:
        """ 
        Get a block using block_number. Optionally include full transaction objects.
        """
        block = self.w3.eth.get_block(block_number, full_transactions=full_transactions)
        return dict(block)
    
    def get_transaction_receipt(self, tx_hash: str) -> Dict[str, Any]:
        """
        Get a transaction receipt using tx_hash.
        """
        receipt = self.w3.eth.get_transaction_receipt(tx_hash)
        return dict(receipt)
    
    def get_block_with_receipts(self, block_number: int) -> Dict[str, Any]:
        """
        Get a block with transaction receipts using block_number.
        """
        block = self.get_block(block_number, full_transactions=True)
        
        # Try to use the eth_getBlockReceipts method if available (QuickNode specific)
        try:
            receipts = self.get_block_receipts(block_number)
            # Create a mapping of transaction hash to receipt
            receipts_map = {r['transactionHash']: r for r in receipts}
            
            # Add receipts to transactions
            for tx in block['transactions']:
                tx_hash = tx['hash'].hex() if hasattr(tx['hash'], 'hex') else tx['hash']
                if tx_hash in receipts_map:
                    tx['receipt'] = receipts_map[tx_hash]
            
            return block
        except Exception:
            # Fallback to fetching individual receipts if batch method not available
            for tx in block['transactions']:
                tx_hash = tx['hash'].hex() if hasattr(tx['hash'], 'hex') else tx['hash']
                tx['receipt'] = self.get_transaction_receipt(tx_hash)
            
            return block
        
    def get_block_formatted(self, block_identifier: Union[int, str], full_transactions: bool = False) -> Dict:
        """
        Get block with formatted data for easier reading.
        """
        if isinstance(block_identifier, int):
            block = self.get_block(block_identifier, full_transactions)
        else:
            # Handle 'latest', 'earliest', etc.
            block = self.w3.eth.get_block(block_identifier, full_transactions=full_transactions)
            block = dict(block)
        
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
        Get a range of blocks using start and end block numbers (inclusive).
        """
        blocks = []
        for block_num in range(start_block, end_block + 1):
            block = self.get_block(block_num, full_transactions)
            blocks.append(block)
        return blocks
    
    def get_transaction_count(self, block_identifier: Union[int, str]) -> int:
        """
        Get the number of transactions in a block.
        """
        return self.w3.eth.get_block_transaction_count(block_identifier)
    
    def get_uncle_count(self, block_identifier: Union[int, str]) -> int:
        """
        Get the number of uncles in a block.
        """
        return self.w3.eth.get_uncle_count(block_identifier)
    
    def get_block_receipts(self, block_number: int) -> List[Dict]:
        """
        Get all transaction receipts for a block (QuickNode specific).
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
        """
        if isinstance(block_identifier, int):
            block = self.get_block(block_identifier)
        else:
            block = self.w3.eth.get_block(block_identifier)
            block = dict(block)
        
        return {
            'gas_used': block['gasUsed'],
            'gas_limit': block['gasLimit'],
            'usage_percentage': (block['gasUsed'] / block['gasLimit']) * 100,
            'base_fee_per_gas': block.get('baseFeePerGas', 0),
            'base_fee_per_gas_gwei': self.w3.from_wei(block.get('baseFeePerGas', 0), 'gwei')
        }
    
    def get_chainlink_price_latest(self) -> Optional[Decimal]:
        """
        Get the latest AVAX/USD price from Chainlink price feed.
        
        Returns:
            Latest AVAX price in USD as Decimal, or None if error
        """
        try:
            # latestRoundData() function selector
            function_selector = "0xfeaf968c"
            
            # Call the contract
            response = self.w3.eth.call({
                'to': self.CHAINLINK_AVAX_USD_FEED,
                'data': function_selector
            })
            
            # Decode the response: (roundId, answer, startedAt, updatedAt, answeredInRound)
            # We only need the answer (index 1)
            decoded = self.w3.codec.decode(['uint80', 'int256', 'uint256', 'uint256', 'uint80'], response)
            raw_price = decoded[1]  # answer field
            
            # Chainlink AVAX/USD has 8 decimal places
            # Convert to Decimal for precision
            price = Decimal(raw_price) / Decimal(10 ** 8)
            
            return price
            
        except Exception as e:
            # Log error but don't raise - let caller handle None return
            return None
    
    def get_chainlink_price_at_block(self, block_number: int) -> Optional[Decimal]:
        """
        Get AVAX/USD price from Chainlink price feed at a specific block.
        
        Args:
            block_number: Block number to query price at
            
        Returns:
            AVAX price in USD as Decimal at the given block, or None if error
        """
        try:
            # latestRoundData() function selector
            function_selector = "0xfeaf968c"
            
            # Call the contract at specific block
            response = self.w3.eth.call({
                'to': self.CHAINLINK_AVAX_USD_FEED,
                'data': function_selector
            }, block_number)
            
            # Decode the response: (roundId, answer, startedAt, updatedAt, answeredInRound)
            # We only need the answer (index 1)
            decoded = self.w3.codec.decode(['uint80', 'int256', 'uint256', 'uint256', 'uint80'], response)
            raw_price = decoded[1]  # answer field
            
            # Chainlink AVAX/USD has 8 decimal places
            # Convert to Decimal for precision
            price = Decimal(raw_price) / Decimal(10 ** 8)
            
            return price
            
        except Exception as e:
            # Log error but don't raise - let caller handle None return
            return None
    
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