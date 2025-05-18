from typing import Dict, Any, List, Optional

from ...utils.logger import get_logger

class TransactionContext:
    def __init__(self, tx: Dict[str, Any]):
        self.tx = tx
        self.tx_hash = tx.get('hash')
        self.logger = get_logger(__name__)
        
        # Extract block data if available
        if 'block' in tx:
            self.block = tx['block']
            self.block_number = self.block.get('number')
            self.timestamp = self.block.get('timestamp')
        else:
            self.block = None
            self.block_number = tx.get('blockNumber')
            self.timestamp = None
            
        # Convert hex values if needed
        if isinstance(self.block_number, str) and self.block_number.startswith('0x'):
            self.block_number = int(self.block_number, 16)
        if isinstance(self.timestamp, str) and self.timestamp.startswith('0x'):
            self.timestamp = int(self.timestamp, 16)
    
    def get_logs(self) -> List[Dict[str, Any]]:
        if 'logs' in self.tx:
            return self.tx['logs']
        if 'receipt' in self.tx and 'logs' in self.tx['receipt']:
            return self.tx['receipt']['logs']
        return []