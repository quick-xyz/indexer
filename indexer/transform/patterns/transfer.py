from typing import Optional, List
from .base import TransferPattern, TransferLeg, AddressContext
from ..context import TransformContext
from ...types import Transfer, Position, ZERO_ADDRESS
from ...utils.amounts import amount_to_negative_str

class Transfer_A(TransferPattern):
    def __init__(self):
        super().__init__("Transfer_A")
    
    def process_signal(self, signal, context):
        # Create Transfer event with positions
        positions = {}
        
        # Position for recipient (positive amount)
        if signal.to_address != ZERO_ADDRESS:
            position_in = Position(
                timestamp=context.transaction.timestamp,
                tx_hash=context.transaction.tx_hash,
                user=signal.to_address,
                token=signal.token,
                amount=signal.amount,
            )
            positions[position_in.content_id] = position_in
        
        # Position for sender (negative amount)
        if signal.from_address != ZERO_ADDRESS:
            position_out = Position(
                timestamp=context.transaction.timestamp,
                tx_hash=context.transaction.tx_hash,
                user=signal.from_address,
                token=signal.token,
                amount=amount_to_negative_str(signal.amount),
            )
            positions[position_out.content_id] = position_out
        
        # Create Transfer event
        transfer = Transfer(
            timestamp=context.transaction.timestamp,
            tx_hash=context.transaction.tx_hash,
            token=signal.token,
            from_address=signal.from_address,
            to_address=signal.to_address,
            amount=signal.amount,
            positions=positions,
            signals={signal.log_index: signal}
        )
        
        # Add event and mark signal as consumed
        context.add_events({transfer.content_id: transfer})
        context.mark_signal_consumed(signal.log_index)
        return True
    
    def _extract_addresses(self, signal, context) -> Optional[AddressContext]:
        return None
    
    def _generate_transfer_legs(self, signal, context) -> Optional[List[TransferLeg]]:
        return []