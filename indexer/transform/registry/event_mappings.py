from .transformer_registry import TransformationRule, TransformationType, registry


def setup_registry():
    """Main setup function to initialize the registry."""
    load_config_mappings()


def load_config_mappings():
    """Load transformation rules and contract mappings from config file."""
    from ..config.config_loader import config_loader
    
    # Load transformation rules from config
    rules = config_loader.get_active_rules()
    for rule_config in rules:
        rule = TransformationRule(
            source_events=rule_config["source_events"],
            target_event=rule_config["target_event"],
            transformation_type=TransformationType(rule_config["transformation_type"]),
            contract_address=rule_config.get("contract_address"),  # None means applies to all
            requires_transfers=rule_config.get("requires_transfers", True),
            transfer_validation=rule_config.get("transfer_validation", True),
            priority=rule_config.get("priority", 0)
        )
        registry.register_transformation_rule(rule)
    
    # Load contract transformer mappings from config
    contracts = config_loader.get_active_contracts()
    for address, contract_config in contracts.items():
        transformer_class = config_loader.get_transformer_class(address)
        if transformer_class:
            registry.register_contract_transformer(address, transformer_class)
    
    # Load any additional transfer event types from config
    settings = config_loader.get_settings()
    additional_transfers = settings.get("additional_transfer_events", [])
    for transfer_event in additional_transfers:
        registry.add_transfer_event_type(transfer_event)