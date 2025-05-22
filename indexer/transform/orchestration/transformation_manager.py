
from typing import List, Dict, Any, Optional, Callable
import logging
from ..context.transaction_context import TransactionContext, DecodedEvent
from ..context.event_buffer import EventBuffer
from ..registry.transformer_registry import registry, TransformationType, TransformationRule
from .dependency_resolver import DependencyResolver

logger = logging.getLogger(__name__)


class TransformationManager:
    """Orchestrates the transformation of decoded events to domain events."""
    
    def __init__(self):
        self.dependency_resolver = DependencyResolver()
        self.event_buffer = EventBuffer()
        self._active_contexts: Dict[str, TransactionContext] = {}
    
    def process_transaction(self, transaction_hash: str, block_number: int, decoded_events: List[DecodedEvent]) -> List[Any]:
        """
        Process all decoded events for a transaction and return domain events.
        """
        # Create transaction context
        context = TransactionContext(transaction_hash, block_number)
        self._active_contexts[transaction_hash] = context
        
        try:
            # Add all decoded events to context
            for event in decoded_events:
                context.add_decoded_event(event)
            
            # Process events through transformation pipeline
            domain_events = self._process_events_pipeline(context)
            
            logger.info(f"Processed transaction {transaction_hash}: {len(domain_events)} domain events created")
            return domain_events
            
        except Exception as e:
            logger.error(f"Error processing transaction {transaction_hash}: {str(e)}")
            raise
        finally:
            # Clean up context
            self._active_contexts.pop(transaction_hash, None)
    
    def _process_events_pipeline(self, context: TransactionContext) -> List[Any]:
        """Main processing pipeline for events in a transaction."""
        
        # Step 1: Process immediate transformations (1:1 and simple M:1)
        self._process_immediate_transformations(context)
        
        # Step 2: Set up buffered transformations for complex M:1 scenarios
        self._setup_buffered_transformations(context)
        
        # Step 3: Process any remaining events
        self._process_remaining_events(context)
        
        # Step 4: Handle timeouts and cleanup
        self._cleanup_buffered_transformations(context)
        
        return context.get_domain_events()
    
    def _process_immediate_transformations(self, context: TransactionContext):
        """Process transformations that can be handled immediately."""
        
        unprocessed_events = context.get_unprocessed_events()
        
        for event in unprocessed_events:
            # Get applicable transformation rules
            rules = self._get_applicable_rules(event, context)
            
            for rule in rules:
                if self._can_process_immediately(rule, event, context):
                    self._execute_transformation(rule, [event], context)
    
    def _setup_buffered_transformations(self, context: TransactionContext):
        """Set up buffered transformations for multi-event scenarios."""
        
        all_events = context.get_all_events()
        processed_rules = set()
        
        for event in all_events:
            rules = self._get_applicable_rules(event, context)
            
            for rule in rules:
                rule_id = self._get_rule_id(rule)
                if (rule_id in processed_rules or 
                    self._can_process_immediately(rule, event, context)):
                    continue
                
                # Create buffered transformation
                transformation_id = f"{context.transaction_hash}_{rule_id}"
                self.event_buffer.create_buffered_transformation(
                    transformation_id=transformation_id,
                    required_events=set(rule.source_events),
                    callback=lambda: self._process_buffered_transformation(rule, context)
                )
                context.add_pending_transformation(transformation_id)
                processed_rules.add(rule_id)
        
        # Feed events to buffer
        for event in all_events:
            ready_transformations = self.event_buffer.add_event_to_buffer(event)
            for transformation_id in ready_transformations:
                self._execute_buffered_transformation(transformation_id, context)
    
    def _process_remaining_events(self, context: TransactionContext):
        """Process any events that haven't been handled yet."""
        
        unprocessed = context.get_unprocessed_events()
        for event in unprocessed:
            # Try to find a default transformation or mark as processed
            if not self._try_default_transformation(event, context):
                logger.warning(f"No transformation found for event {event.event_name} in {context.transaction_hash}")
                context.mark_event_processed(event)
    
    def _cleanup_buffered_transformations(self, context: TransactionContext):
        """Clean up any timed out or incomplete buffered transformations."""
        
        timed_out = self.event_buffer.cleanup_timed_out_transformations()
        for transformation_id in timed_out:
            context.remove_pending_transformation(transformation_id)
            logger.warning(f"Buffered transformation {transformation_id} timed out")
    
    def _get_applicable_rules(self, event: DecodedEvent, context: TransactionContext) -> List[TransformationRule]:
        """Get transformation rules that apply to an event."""
        
        # Get rules triggered by this event type
        triggered_rules = registry.get_triggered_rules(event.event_name)
        
        # Filter by contract if rule specifies one
        applicable_rules = []
        for rule in triggered_rules:
            if (rule.contract_address is None or 
                rule.contract_address.lower() == event.contract_address.lower()):
                applicable_rules.append(rule)
        
        # Sort by priority
        return sorted(applicable_rules, key=lambda r: r.priority, reverse=True)
    
    def _can_process_immediately(self, rule: TransformationRule, event: DecodedEvent, context: TransactionContext) -> bool:
        """Check if a transformation rule can be processed immediately."""
        
        if rule.transformation_type == TransformationType.ONE_TO_ONE:
            return True
        
        if rule.transformation_type == TransformationType.MANY_TO_ONE:
            if not rule.requires_all_sources:
                return True
            
            # Check if all required source events are present
            for required_event in rule.source_events:
                if not context.get_events_by_name(required_event):
                    return False
            return True
        
        return False
    
    def _execute_transformation(self, rule: TransformationRule, events: List[DecodedEvent], context: TransactionContext):
        """Execute a transformation rule with the given events."""
        
        try:
            # Get contract-specific transformer if available
            transformer_class = None
            if events:
                transformer_class = registry.get_contract_transformer(events[0].contract_address)
            
            if transformer_class:
                transformer = transformer_class()
                domain_events = transformer.transform(rule, events, context)
            else:
                # Use default transformation logic
                domain_events = self._default_transform(rule, events, context)
            
            # Add domain events to context
            for domain_event in domain_events:
                context.add_domain_event(domain_event)
            
            # Mark events as processed
            for event in events:
                context.mark_event_processed(event, [type(de).__name__ for de in domain_events])
            
            logger.debug(f"Executed transformation {rule.target_event} with {len(events)} events")
            
        except Exception as e:
            logger.error(f"Error executing transformation {rule.target_event}: {str(e)}")
            for event in events:
                context.mark_event_error(event, str(e))
    
    def _execute_buffered_transformation(self, transformation_id: str, context: TransactionContext):
        """Execute a buffered transformation that's now ready."""
        
        buffered = self.event_buffer.get_buffered_transformation(transformation_id)
        if not buffered:
            return
        
        # Collect all events for this transformation
        all_events = []
        for event_list in buffered.collected_events.values():
            all_events.extend(event_list)
        
        # Find the rule (this is simplified - in practice you'd store the rule with the buffered transformation)
        rule = self._find_rule_for_transformation(transformation_id, all_events)
        if rule:
            self._execute_transformation(rule, all_events, context)
        
        # Clean up
        self.event_buffer.complete_transformation(transformation_id)
        context.remove_pending_transformation(transformation_id)
    
    def _default_transform(self, rule: TransformationRule, events: List[DecodedEvent], context: TransactionContext) -> List[Any]:
        """Default transformation logic when no contract-specific transformer is available."""
        # This is a placeholder - implement your default transformation logic
        logger.warning(f"Using default transformation for {rule.target_event}")
        return []
    
    def _try_default_transformation(self, event: DecodedEvent, context: TransactionContext) -> bool:
        """Try to apply a default transformation to an unprocessed event."""
        # Implement default handling logic
        return False
    
    def _get_rule_id(self, rule: TransformationRule) -> str:
        """Generate a unique ID for a transformation rule."""
        return f"{rule.target_event}_{hash(frozenset(rule.source_events))}"
    
    def _find_rule_for_transformation(self, transformation_id: str, events: List[DecodedEvent]) -> Optional[TransformationRule]:
        """Find the transformation rule for a buffered transformation."""
        # This is simplified - in practice you'd store the rule with the buffered transformation
        if events:
            return self._get_applicable_rules(events[0], self._active_contexts.get(events[0].transaction_hash))[0]
        return None