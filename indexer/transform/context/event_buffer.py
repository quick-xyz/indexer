
from typing import Dict, List, Set, Optional, Callable
from dataclasses import dataclass, field
from collections import defaultdict
from .transaction_context import DecodedEvent
import time


@dataclass
class BufferedTransformation:
    """Represents a transformation waiting for additional events."""
    transformation_id: str
    required_events: Set[str]  # Event names needed
    collected_events: Dict[str, List[DecodedEvent]] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    timeout_seconds: Optional[float] = None
    callback: Optional[Callable] = None


class EventBuffer:
    """Buffers events for multi-event transformations."""
    
    def __init__(self):
        self._buffered_transformations: Dict[str, BufferedTransformation] = {}
        self._event_to_transformations: Dict[str, Set[str]] = defaultdict(set)
    
    def create_buffered_transformation(
        self, 
        transformation_id: str, 
        required_events: Set[str],
        timeout_seconds: Optional[float] = None,
        callback: Optional[Callable] = None
    ) -> BufferedTransformation:
        """Create a new buffered transformation waiting for events."""
        
        buffered = BufferedTransformation(
            transformation_id=transformation_id,
            required_events=required_events,
            timeout_seconds=timeout_seconds,
            callback=callback
        )
        
        self._buffered_transformations[transformation_id] = buffered
        
        # Index by event names for quick lookup
        for event_name in required_events:
            self._event_to_transformations[event_name].add(transformation_id)
        
        return buffered
    
    def add_event_to_buffer(self, event: DecodedEvent) -> List[str]:
        """
        Add an event to relevant buffered transformations.
        Returns list of transformation IDs that are now ready.
        """
        ready_transformations = []
        
        # Find transformations waiting for this event type
        transformation_ids = self._event_to_transformations.get(event.event_name, set())
        
        for transformation_id in transformation_ids:
            buffered = self._buffered_transformations.get(transformation_id)
            if not buffered:
                continue
            
            # Add event to the buffered transformation
            if event.event_name not in buffered.collected_events:
                buffered.collected_events[event.event_name] = []
            buffered.collected_events[event.event_name].append(event)
            
            # Check if transformation is now ready
            if self._is_transformation_ready(buffered):
                ready_transformations.append(transformation_id)
        
        return ready_transformations
    
    def _is_transformation_ready(self, buffered: BufferedTransformation) -> bool:
        """Check if a buffered transformation has all required events."""
        collected_event_types = set(buffered.collected_events.keys())
        return buffered.required_events.issubset(collected_event_types)
    
    def get_buffered_transformation(self, transformation_id: str) -> Optional[BufferedTransformation]:
        """Get a buffered transformation by ID."""
        return self._buffered_transformations.get(transformation_id)
    
    def complete_transformation(self, transformation_id: str):
        """Mark a transformation as complete and remove from buffer."""
        buffered = self._buffered_transformations.pop(transformation_id, None)
        if buffered:
            # Clean up event mappings
            for event_name in buffered.required_events:
                self._event_to_transformations[event_name].discard(transformation_id)
                # Clean up empty sets
                if not self._event_to_transformations[event_name]:
                    del self._event_to_transformations[event_name]
    
    def get_timed_out_transformations(self) -> List[str]:
        """Get transformations that have exceeded their timeout."""
        current_time = time.time()
        timed_out = []
        
        for transformation_id, buffered in self._buffered_transformations.items():
            if (buffered.timeout_seconds and 
                current_time - buffered.created_at > buffered.timeout_seconds):
                timed_out.append(transformation_id)
        
        return timed_out
    
    def cleanup_timed_out_transformations(self) -> List[str]:
        """Remove and return timed out transformations."""
        timed_out = self.get_timed_out_transformations()
        for transformation_id in timed_out:
            self.complete_transformation(transformation_id)
        return timed_out
    
    def get_pending_count(self) -> int:
        """Get number of pending buffered transformations."""
        return len(self._buffered_transformations)
    
    def get_buffer_summary(self) -> Dict[str, any]:
        """Get summary of buffer state."""
        return {
            "pending_transformations": len(self._buffered_transformations),
            "transformation_details": {
                tid: {
                    "required_events": list(buffered.required_events),
                    "collected_events": list(buffered.collected_events.keys()),
                    "is_ready": self._is_transformation_ready(buffered),
                    "age_seconds": time.time() - buffered.created_at
                }
                for tid, buffered in self._buffered_transformations.items()
            }
        }