# indexer/cli/commands/pool_pricing_commands.py

from typing import Optional
from sqlalchemy import and_

from ...database.connection import DatabaseManager
from ...database.shared.tables.config import Model, Contract
from ...database.shared.tables.pool_pricing_config import PoolPricingConfig
from ...database.shared.repositories.pool_pricing_config_repository import PoolPricingConfigRepository
from ...core.logging_config import IndexerLogger, log_with_context

import logging


class PoolPricingCommands:
    """CLI commands for managing pool pricing configurations"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager  # Should be shared database manager
        self.repository = PoolPricingConfigRepository(db_manager)
        self.logger = IndexerLogger.get_logger('cli.pool_pricing_commands')
    
    def add_pool_config(
        self,
        model_name: str,
        pool_address: str,
        start_block: int,
        pricing_strategy: str = 'GLOBAL',
        primary_pool: bool = False,
        end_block: Optional[int] = None,
        quote_token_address: Optional[str] = None,
        quote_token_type: Optional[str] = None,
        created_by: Optional[str] = None,
        notes: Optional[str] = None
    ) -> bool:
        """Add a new pool pricing configuration"""
        try:
            with self.db_manager.get_session() as session:
                # Get model
                model = session.query(Model).filter(
                    and_(Model.name == model_name, Model.status == 'active')
                ).first()
                
                if not model:
                    print(f"❌ Model '{model_name}' not found")
                    return False
                
                # Get contract
                contract = session.query(Contract).filter(
                    Contract.address == pool_address.lower()
                ).first()
                
                if not contract:
                    print(f"❌ Contract '{pool_address}' not found")
                    return False
                
                # Validate pricing strategy
                if pricing_strategy not in ['DIRECT', 'GLOBAL']:
                    print(f"❌ Invalid pricing strategy '{pricing_strategy}'. Must be 'DIRECT' or 'GLOBAL'")
                    return False
                
                # Validate quote token type
                if quote_token_type and quote_token_type not in ['AVAX', 'USD_EQUIVALENT', 'OTHER']:
                    print(f"❌ Invalid quote token type '{quote_token_type}'. Must be 'AVAX', 'USD_EQUIVALENT', or 'OTHER'")
                    return False
                
                # Create configuration
                config = self.repository.create_config(
                    session=session,
                    model_id=model.id,
                    contract_id=contract.id,
                    start_block=start_block,
                    pricing_strategy=pricing_strategy,
                    primary_pool=primary_pool,
                    end_block=end_block,
                    quote_token_address=quote_token_address,
                    quote_token_type=quote_token_type,
                    created_by=created_by,
                    notes=notes
                )
                
                session.commit()
                
                print(f"✅ Pool pricing configuration created")
                print(f"   Model: {model_name}")
                print(f"   Pool: {pool_address}")
                print(f"   Strategy: {pricing_strategy}")
                print(f"   Primary Pool: {primary_pool}")
                print(f"   Block Range: {start_block} - {end_block or '∞'}")
                if quote_token_address:
                    print(f"   Quote Token: {quote_token_address} ({quote_token_type})")
                
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to add pool configuration: {e}")
            print(f"❌ Error: {e}")
            return False
    
    def close_pool_config(
        self,
        model_name: str,
        pool_address: str,
        end_block: int,
        notes: Optional[str] = None
    ) -> bool:
        """Close the active configuration for a pool"""
        try:
            with self.db_manager.get_session() as session:
                # Get model
                model = session.query(Model).filter(
                    and_(Model.name == model_name, Model.status == 'active')
                ).first()
                
                if not model:
                    print(f"❌ Model '{model_name}' not found")
                    return False
                
                # Get contract
                contract = session.query(Contract).filter(
                    Contract.address == pool_address.lower()
                ).first()
                
                if not contract:
                    print(f"❌ Contract '{pool_address}' not found")
                    return False
                
                # Find active configuration
                active_config = session.query(PoolPricingConfig).filter(
                    PoolPricingConfig.model_id == model.id,
                    PoolPricingConfig.contract_id == contract.id,
                    PoolPricingConfig.end_block.is_(None)
                ).first()
                
                if not active_config:
                    print(f"❌ No active configuration found for pool '{pool_address}' in model '{model_name}'")
                    return False
                
                # Close configuration
                success = self.repository.close_config(
                    session=session,
                    config_id=active_config.id,
                    end_block=end_block,
                    notes=notes
                )
                
                if success:
                    session.commit()
                    print(f"✅ Pool configuration closed at block {end_block}")
                    print(f"   Model: {model_name}")
                    print(f"   Pool: {pool_address}")
                    return True
                else:
                    return False
                
        except Exception as e:
            self.logger.error(f"Failed to close pool configuration: {e}")
            print(f"❌ Error: {e}")
            return False
    
    def show_pool_config(self, model_name: str, pool_address: str):
        """Show all configurations for a specific pool"""
        try:
            with self.db_manager.get_session() as session:
                # Get model
                model = session.query(Model).filter(
                    and_(Model.name == model_name, Model.status == 'active')
                ).first()
                
                if not model:
                    print(f"❌ Model '{model_name}' not found")
                    return
                
                # Get contract
                contract = session.query(Contract).filter(
                    Contract.address == pool_address.lower()
                ).first()
                
                if not contract:
                    print(f"❌ Contract '{pool_address}' not found")
                    return
                
                # Get all configurations
                configs = self.repository.get_configs_for_pool(
                    session=session,
                    model_id=model.id,
                    contract_id=contract.id
                )
                
                if not configs:
                    print(f"📝 No pricing configurations found for pool '{pool_address}' in model '{model_name}'")
                    print(f"   Default: GLOBAL pricing strategy")
                    return
                
                print(f"📝 Pool Pricing Configurations: {contract.name}")
                print(f"   Address: {pool_address}")
                print(f"   Model: {model_name}")
                print()
                
                for i, config in enumerate(configs, 1):
                    status = "ACTIVE" if config.end_block is None else "CLOSED"
                    end_str = "∞" if config.end_block is None else str(config.end_block)
                    
                    print(f"   {i}. [{status}] Blocks {config.start_block} - {end_str}")
                    print(f"      Strategy: {config.pricing_strategy}")
                    print(f"      Primary Pool: {config.primary_pool}")
                    if config.quote_token_address:
                        print(f"      Quote Token: {config.quote_token_address} ({config.quote_token_type})")
                    if config.created_by:
                        print(f"      Created By: {config.created_by}")
                    if config.notes:
                        print(f"      Notes: {config.notes}")
                    print()
                
        except Exception as e:
            self.logger.error(f"Failed to show pool configuration: {e}")
            print(f"❌ Error: {e}")
    
    def list_pool_configs(self, model_name: str, strategy_filter: Optional[str] = None):
        """List all pool configurations for a model"""
        try:
            with self.db_manager.get_session() as session:
                # Get model
                model = session.query(Model).filter(
                    and_(Model.name == model_name, Model.status == 'active')
                ).first()
                
                if not model:
                    print(f"❌ Model '{model_name}' not found")
                    return
                
                # Get all configurations with optional filter
                configs = self.repository.get_configs_for_model(
                    session=session,
                    model_id=model.id,
                    strategy_filter=strategy_filter
                )
                
                if not configs:
                    filter_str = f" with strategy '{strategy_filter}'" if strategy_filter else ""
                    print(f"📝 No pool configurations found for model '{model_name}'{filter_str}")
                    return
                
                # Get stats
                stats = self.repository.get_configuration_stats(session, model.id)
                
                print(f"📝 Pool Pricing Configurations for Model: {model_name}")
                print(f"   Total Configurations: {stats['total_configurations']}")
                print(f"   Direct Pricing: {stats['direct_pricing_configurations']}")
                print(f"   Global Pricing: {stats['global_pricing_configurations']}")
                print(f"   Primary Pools: {stats['primary_pool_configurations']}")
                print(f"   Active Configurations: {stats['active_configurations']}")
                print()
                
                # Group by contract for display
                current_contract = None
                for config in configs:
                    if current_contract != config.contract.name:
                        current_contract = config.contract.name
                        print(f"🏊 {config.contract.name} ({config.contract.address})")
                    
                    status = "ACTIVE" if config.end_block is None else "CLOSED"
                    end_str = "∞" if config.end_block is None else str(config.end_block)
                    primary_str = " [PRIMARY]" if config.primary_pool else ""
                    
                    print(f"   {config.start_block:>8} - {end_str:<8} | {config.pricing_strategy:<8} | {status}{primary_str}")
                    
                    if config.quote_token_address:
                        print(f"            Quote: {config.quote_token_address} ({config.quote_token_type})")
                
        except Exception as e:
            self.logger.error(f"Failed to list pool configurations: {e}")
            print(f"❌ Error: {e}")
    
    def get_pool_config_at_block(self, model_name: str, pool_address: str, block_number: int):
        """Show the active configuration for a pool at a specific block"""
        try:
            with self.db_manager.get_session() as session:
                # Get model
                model = session.query(Model).filter(
                    and_(Model.name == model_name, Model.status == 'active')
                ).first()
                
                if not model:
                    print(f"❌ Model '{model_name}' not found")
                    return
                
                # Get contract
                contract = session.query(Contract).filter(
                    Contract.address == pool_address.lower()
                ).first()
                
                if not contract:
                    print(f"❌ Contract '{pool_address}' not found")
                    return
                
                # Get active configuration at block
                config = self.repository.get_active_config_for_pool(
                    session=session,
                    model_id=model.id,
                    contract_id=contract.id,
                    block_number=block_number
                )
                
                print(f"📝 Pool Configuration at Block {block_number:,}")
                print(f"   Pool: {contract.name} ({pool_address})")
                print(f"   Model: {model_name}")
                print()
                
                if config:
                    end_str = "∞" if config.end_block is None else str(config.end_block)
                    print(f"   Configuration Found:")
                    print(f"     Block Range: {config.start_block} - {end_str}")
                    print(f"     Strategy: {config.pricing_strategy}")
                    print(f"     Primary Pool: {config.primary_pool}")
                    if config.quote_token_address:
                        print(f"     Quote Token: {config.quote_token_address} ({config.quote_token_type})")
                    if config.notes:
                        print(f"     Notes: {config.notes}")
                else:
                    print(f"   No Configuration Found")
                    print(f"     Default: GLOBAL pricing strategy")
                
        except Exception as e:
            self.logger.error(f"Failed to get pool configuration: {e}")
            print(f"❌ Error: {e}")
    
    def show_primary_pools(self, model_name: str, block_number: Optional[int] = None):
        """Show all primary pools for canonical pricing at a specific block"""
        try:
            with self.db_manager.get_session() as session:
                # Get model
                model = session.query(Model).filter(
                    and_(Model.name == model_name, Model.status == 'active')
                ).first()
                
                if not model:
                    print(f"❌ Model '{model_name}' not found")
                    return
                
                # Use latest block if not specified
                if block_number is None:
                    from ...clients.quicknode_rpc import QuickNodeRpcClient
                    # This would need to be injected properly in a real implementation
                    print("⚠️  Block number not specified, showing all primary pool configurations")
                    
                    # Get all primary pool configurations
                    primary_configs = self.repository.get_configs_for_model(
                        session=session,
                        model_id=model.id,
                        primary_only=True
                    )
                else:
                    # Get primary pools at specific block
                    primary_configs = self.repository.get_primary_pools_at_block(
                        session=session,
                        model_id=model.id,
                        block_number=block_number
                    )
                
                if not primary_configs:
                    block_str = f" at block {block_number:,}" if block_number else ""
                    print(f"📝 No primary pools configured for model '{model_name}'{block_str}")
                    return
                
                block_str = f" at Block {block_number:,}" if block_number else ""
                print(f"🏆 Primary Pools for Canonical Pricing{block_str}")
                print(f"   Model: {model_name}")
                print()
                
                for config in primary_configs:
                    status = "ACTIVE" if config.end_block is None else "CLOSED"
                    end_str = "∞" if config.end_block is None else str(config.end_block)
                    
                    print(f"   🏊 {config.contract.name}")
                    print(f"      Address: {config.contract.address}")
                    print(f"      Block Range: {config.start_block} - {end_str} [{status}]")
                    print(f"      Strategy: {config.pricing_strategy}")
                    if config.quote_token_address:
                        print(f"      Quote Token: {config.quote_token_address} ({config.quote_token_type})")
                    print()
                
        except Exception as e:
            self.logger.error(f"Failed to show primary pools: {e}")
            print(f"❌ Error: {e}")
    
    def validate_pool_configs(self, model_name: str):
        """Validate all pool configurations for a model"""
        try:
            with self.db_manager.get_session() as session:
                # Get model
                model = session.query(Model).filter(
                    and_(Model.name == model_name, Model.status == 'active')
                ).first()
                
                if not model:
                    print(f"❌ Model '{model_name}' not found")
                    return
                
                # Get all configurations
                configs = self.repository.get_configs_for_model(
                    session=session,
                    model_id=model.id
                )
                
                print(f"🔍 Validating Pool Configurations for Model: {model_name}")
                print("=" * 60)
                
                total_configs = len(configs)
                valid_configs = 0
                invalid_configs = 0
                
                for config in configs:
                    validation_errors = config.validate_config()
                    
                    if validation_errors:
                        invalid_configs += 1
                        status_icon = "❌"
                        print(f"{status_icon} {config.contract.name} ({config.contract.address})")
                        print(f"     Block Range: {config.block_range_str}")
                        print(f"     Errors:")
                        for error in validation_errors:
                            print(f"       - {error}")
                        print()
                    else:
                        valid_configs += 1
                        status_icon = "✅"
                        print(f"{status_icon} {config.contract.name} ({config.contract.address})")
                
                print("=" * 60)
                print(f"📊 Validation Summary:")
                print(f"   Total Configurations: {total_configs}")
                print(f"   Valid: {valid_configs}")
                print(f"   Invalid: {invalid_configs}")
                
                if invalid_configs > 0:
                    print(f"\n⚠️  {invalid_configs} configuration(s) need attention!")
                else:
                    print(f"\n🎉 All configurations are valid!")
                
        except Exception as e:
            self.logger.error(f"Failed to validate pool configurations: {e}")
            print(f"❌ Error: {e}")