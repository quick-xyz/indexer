from typing import Optional, List
from .base import TransferPattern, TransferLeg, AddressContext
from ..context import TransformContext

class Route_A(TransferPattern):
    def __init__(self):
        super().__init__("Route_A")
    
    def process_signal(self, signal, context):
        # Route signals just provide context
        context.mark_signal_consumed(signal.log_index)
        return True
    
    def _extract_addresses(self, signal, context) -> Optional[AddressContext]:
        return None
    
    def _generate_transfer_legs(self, signal, context) -> Optional[List[TransferLeg]]:
        return []