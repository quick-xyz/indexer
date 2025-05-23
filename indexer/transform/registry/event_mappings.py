from .transformer_registry import TransformationRule, TransformationType, registry


def initialize_event_mappings():
    # Example: Simple 1:1 transformation
    registry.register_transformation_rule(TransformationRule(
        source_events=["Transfer"],
        target_event="TokenTransferEvent",
        transformation_type=TransformationType.ONE_TO_ONE,
        priority=1
    ))
    
    # Example: Many-to-one transformation (e.g., swap events)
    registry.register_transformation_rule(TransformationRule(
        source_events=["Deposit", "Withdrawal", "Swap"],
        target_event="LiquidityChangeEvent",
        transformation_type=TransformationType.MANY_TO_ONE,
        requires_all_sources=False,  # Any of these events can trigger
        priority=2
    ))
    
    # Example: Contract-specific transformation
    registry.register_transformation_rule(TransformationRule(
        source_events=["OrderFilled"],
        target_event="TradeExecutedEvent",
        transformation_type=TransformationType.ONE_TO_ONE,
        contract_address="0x1234567890abcdef1234567890abcdef12345678",
        priority=3
    ))
    
    # Example: One-to-many transformation
    registry.register_transformation_rule(TransformationRule(
        source_events=["LargeTransfer"],
        target_event="MultipleAlertEvents",
        transformation_type=TransformationType.ONE_TO_MANY,
        priority=1
    ))


def register_contract_transformers():
    """Register contract-specific transformer classes."""
    # These would be imported from your contract_transformers directory
    # from ..contract_transformers import ERC20Transformer, UniswapTransformer
    
    # Example registrations:
    # registry.register_contract_transformer(
    #     "0x1234567890abcdef1234567890abcdef12345678", 
    #     ERC20Transformer
    # )
    # registry.register_contract_transformer(
    #     "0xabcdef1234567890abcdef1234567890abcdef12", 
    #     UniswapTransformer
    # )
    pass


def setup_registry():
    initialize_event_mappings()
    register_contract_transformers()