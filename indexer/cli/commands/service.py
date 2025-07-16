# indexer/cli/commands/service.py

"""
Service Management CLI Commands

Integrates PricingService and CalculationService operations via ServiceRunner.
Replaces the deprecated pricing_service_runner.py with enhanced functionality.
"""

import click
from typing import Optional

@click.group()
def service():
    """Service operations for pricing and calculation"""
    pass


# =====================================================================
# PRICING SERVICE COMMANDS
# =====================================================================

@service.group()
def pricing():
    """Pricing service operations and management"""
    pass


@pricing.command('update-canonical')
@click.option('--asset', required=True, help='Asset address to update canonical pricing for')
@click.option('--minutes', type=int, help='Number of minutes to process (from most recent)')
@click.option('--denomination', type=click.Choice(['usd', 'avax']), help='Denomination to process')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def update_canonical(ctx, asset, minutes, denomination, model):
    """Update canonical pricing using 5-minute VWAP from pricing pools
    
    Generates canonical price authority records in price_vwap table using
    volume-weighted average prices from designated pricing pools.
    
    Examples:
        # Update canonical pricing for last 24 hours
        service pricing update-canonical --asset 0x1234... --minutes 1440
        
        # Update USD denomination only
        service pricing update-canonical --asset 0x1234... --denomination usd
        
        # Update all gaps (no time limit)
        service pricing update-canonical --asset 0x1234...
        
        # Specific model
        service pricing update-canonical --asset 0x1234... --model blub_test
    """
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    try:
        from ...services.service_runner import ServiceRunner
        
        runner = ServiceRunner(model_name=model_name)
        runner.run_canonical_pricing_update(
            asset_address=asset,
            minutes=minutes,
            denomination=denomination
        )
        
    except Exception as e:
        raise click.ClickException(f"Canonical pricing update failed: {e}")


@pricing.command('update-global')
@click.option('--asset', required=True, help='Asset address to update global pricing for')
@click.option('--days', type=int, help='Number of days to look back for unpriced events')
@click.option('--denomination', type=click.Choice(['usd', 'avax']), help='Denomination to process')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def update_global(ctx, asset, days, denomination, model):
    """Apply canonical pricing to events without direct pricing
    
    Finds pool swaps and trades that lack direct pricing and applies
    canonical prices from price_vwap table with GLOBAL pricing method.
    
    Examples:
        # Update global pricing for last 7 days
        service pricing update-global --asset 0x1234... --days 7
        
        # Update AVAX denomination only
        service pricing update-global --asset 0x1234... --denomination avax
        
        # Update all unpriced events
        service pricing update-global --asset 0x1234...
    """
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    try:
        from ...services.service_runner import ServiceRunner
        
        runner = ServiceRunner(model_name=model_name)
        runner.run_global_pricing_update(
            asset_address=asset,
            days=days,
            denomination=denomination
        )
        
    except Exception as e:
        raise click.ClickException(f"Global pricing update failed: {e}")


@pricing.command('update-all')
@click.option('--asset', required=True, help='Asset address to update all pricing for')
@click.option('--days', type=int, help='Number of days to look back for gaps')
@click.option('--denomination', type=click.Choice(['usd', 'avax']), help='Denomination to process')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def update_all_pricing(ctx, asset, days, denomination, model):
    """Comprehensive pricing update (infrastructure + direct + canonical + global)
    
    Runs the complete pricing pipeline:
    1. Infrastructure updates (periods, block prices)
    2. Direct pricing (pool swaps, trades)
    3. Canonical pricing (VWAP generation)
    4. Global pricing (apply canonical to unpriced events)
    
    Examples:
        # Complete pricing update for asset
        service pricing update-all --asset 0x1234...
        
        # Update last 30 days only
        service pricing update-all --asset 0x1234... --days 30
        
        # USD denomination only
        service pricing update-all --asset 0x1234... --denomination usd
    """
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    try:
        from ...services.service_runner import ServiceRunner
        
        runner = ServiceRunner(model_name=model_name)
        runner.run_pricing_update_all(
            asset_address=asset,
            days=days,
            denomination=denomination
        )
        
    except Exception as e:
        raise click.ClickException(f"Comprehensive pricing update failed: {e}")


@pricing.command('status')
@click.option('--asset', required=True, help='Asset address to check pricing status for')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def pricing_status(ctx, asset, model):
    """Show comprehensive pricing status for an asset
    
    Displays detailed pricing coverage statistics including:
    - Canonical pricing status and gaps
    - Direct pricing coverage  
    - Global pricing gaps
    - Recent activity timestamps
    
    Examples:
        # Check pricing status
        service pricing status --asset 0x1234...
        
        # Specific model
        service pricing status --asset 0x1234... --model blub_test
    """
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    try:
        from ...services.service_runner import ServiceRunner
        
        runner = ServiceRunner(model_name=model_name)
        runner.show_pricing_status(asset_address=asset)
        
    except Exception as e:
        raise click.ClickException(f"Failed to get pricing status: {e}")


# =====================================================================
# CALCULATION SERVICE COMMANDS
# =====================================================================

@service.group()
def calculation():
    """Calculation service operations and management"""
    pass


@calculation.command('update-events')
@click.option('--asset', required=True, help='Asset address to update event valuations for')
@click.option('--days', type=int, help='Number of days to look back for unvalued events')
@click.option('--denomination', type=click.Choice(['usd', 'avax']), help='Denomination to process')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def update_events(ctx, asset, days, denomination, model):
    """Update event valuations using canonical pricing
    
    Applies canonical prices to value transfers, liquidity events, rewards,
    and positions, creating event_details records with USD/AVAX valuations.
    
    Examples:
        # Update event valuations for last 7 days
        service calculation update-events --asset 0x1234... --days 7
        
        # Update USD valuations only
        service calculation update-events --asset 0x1234... --denomination usd
        
        # Update all unvalued events
        service calculation update-events --asset 0x1234...
    """
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    try:
        from ...services.service_runner import ServiceRunner
        
        runner = ServiceRunner(model_name=model_name)
        runner.run_event_valuations_update(
            asset_address=asset,
            days=days,
            denomination=denomination
        )
        
    except Exception as e:
        raise click.ClickException(f"Event valuations update failed: {e}")


@calculation.command('update-analytics')
@click.option('--asset', required=True, help='Asset address to update analytics for')
@click.option('--days', type=int, help='Number of days to look back for missing analytics')
@click.option('--denomination', type=click.Choice(['usd', 'avax']), help='Denomination to process')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def update_analytics(ctx, asset, days, denomination, model):
    """Update analytics (OHLC candles + protocol volume metrics)
    
    Generates:
    - OHLC candles from trade aggregation (asset_price table)
    - Protocol-level volume metrics using contract.project (asset_volume table)
    
    Examples:
        # Update analytics for last 7 days
        service calculation update-analytics --asset 0x1234... --days 7
        
        # Update AVAX analytics only
        service calculation update-analytics --asset 0x1234... --denomination avax
        
        # Update all missing analytics
        service calculation update-analytics --asset 0x1234...
    """
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    try:
        from ...services.service_runner import ServiceRunner
        
        runner = ServiceRunner(model_name=model_name)
        runner.run_analytics_update(
            asset_address=asset,
            days=days,
            denomination=denomination
        )
        
    except Exception as e:
        raise click.ClickException(f"Analytics update failed: {e}")


@calculation.command('update-all')
@click.option('--asset', required=True, help='Asset address to update all calculations for')
@click.option('--days', type=int, help='Number of days to look back for gaps')
@click.option('--denomination', type=click.Choice(['usd', 'avax']), help='Denomination to process')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def update_all_calculation(ctx, asset, days, denomination, model):
    """Comprehensive calculation update (event valuations + analytics)
    
    Runs the complete calculation pipeline:
    1. Event valuations (transfers, liquidity, rewards, positions)
    2. Analytics generation (OHLC candles, protocol volume metrics)
    
    Examples:
        # Complete calculation update
        service calculation update-all --asset 0x1234...
        
        # Update last 14 days only
        service calculation update-all --asset 0x1234... --days 14
        
        # USD denomination only
        service calculation update-all --asset 0x1234... --denomination usd
    """
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    try:
        from ...services.service_runner import ServiceRunner
        
        runner = ServiceRunner(model_name=model_name)
        runner.run_calculation_update_all(
            asset_address=asset,
            days=days,
            denomination=denomination
        )
        
    except Exception as e:
        raise click.ClickException(f"Comprehensive calculation update failed: {e}")


@calculation.command('status')
@click.option('--asset', required=True, help='Asset address to check calculation status for')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def calculation_status(ctx, asset, model):
    """Show comprehensive calculation status for an asset
    
    Displays detailed calculation coverage statistics including:
    - Event valuation status and gaps
    - Analytics coverage (OHLC candles, volume metrics)
    - Recent activity timestamps
    
    Examples:
        # Check calculation status
        service calculation status --asset 0x1234...
        
        # Specific model
        service calculation status --asset 0x1234... --model blub_test
    """
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    try:
        from ...services.service_runner import ServiceRunner
        
        runner = ServiceRunner(model_name=model_name)
        runner.show_calculation_status(asset_address=asset)
        
    except Exception as e:
        raise click.ClickException(f"Failed to get calculation status: {e}")


# =====================================================================
# COMPREHENSIVE SERVICE COMMANDS
# =====================================================================

@service.command('update-all')
@click.option('--asset', required=True, help='Asset address to update all services for')
@click.option('--days', type=int, help='Number of days to look back for gaps')
@click.option('--denomination', type=click.Choice(['usd', 'avax']), help='Denomination to process')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def update_all_services(ctx, asset, days, denomination, model):
    """Update all services (pricing + calculation) comprehensively
    
    Runs the complete service ecosystem:
    1. Pricing Service: Infrastructure + Direct + Canonical + Global pricing
    2. Calculation Service: Event valuations + Analytics generation
    
    This is the main command for comprehensive asset processing.
    
    Examples:
        # Complete service update for asset
        service update-all --asset 0x1234...
        
        # Update last 30 days only
        service update-all --asset 0x1234... --days 30
        
        # USD denomination only
        service update-all --asset 0x1234... --denomination usd
        
        # Specific model
        service update-all --asset 0x1234... --model blub_test
    """
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    try:
        from ...services.service_runner import ServiceRunner
        
        runner = ServiceRunner(model_name=model_name)
        runner.run_update_all_services(
            asset_address=asset,
            days=days,
            denomination=denomination
        )
        
    except Exception as e:
        raise click.ClickException(f"Comprehensive service update failed: {e}")


@service.command('status')
@click.option('--asset', required=True, help='Asset address to check all service status for')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def service_status(ctx, asset, model):
    """Show comprehensive status for all services (pricing + calculation)
    
    Displays complete service coverage including:
    - Pricing service status (canonical, direct, global)
    - Calculation service status (valuations, analytics)
    - Cross-service gap analysis
    - Recent activity across all services
    
    Examples:
        # Check all service status
        service status --asset 0x1234...
        
        # Specific model
        service status --asset 0x1234... --model blub_test
    """
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    try:
        from ...services.service_runner import ServiceRunner
        
        runner = ServiceRunner(model_name=model_name)
        runner.show_service_status(asset_address=asset)
        
    except Exception as e:
        raise click.ClickException(f"Failed to get service status: {e}")


# =====================================================================
# LEGACY COMPATIBILITY (for existing pricing commands)
# =====================================================================

@service.group()
def legacy():
    """Legacy pricing commands for backwards compatibility"""
    pass


@legacy.command('update-periods')
@click.option('--types', help='Comma-separated period types (1min,5min,1hr,1day)')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def legacy_update_periods(ctx, types, model):
    """Update periods to present time (legacy compatibility)
    
    Examples:
        # Update all period types
        service legacy update-periods
        
        # Update specific period types  
        service legacy update-periods --types 5min,1hr
    """
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    try:
        from ...services.service_runner import ServiceRunner
        
        runner = ServiceRunner(model_name=model_name)
        
        # Use the pricing service's infrastructure update methods
        click.echo("🕐 Updating periods via pricing service...")
        
        # This leverages the PricingService.update_periods_to_present() method
        results = runner.pricing_service.update_periods_to_present()
        
        click.echo("✅ Period update complete")
        for period_type, count in results.items():
            click.echo(f"  • {period_type}: {count} periods created")
        
    except Exception as e:
        raise click.ClickException(f"Period update failed: {e}")


@legacy.command('update-prices')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def legacy_update_prices(ctx, model):
    """Update minute-by-minute AVAX prices (legacy compatibility)
    
    Examples:
        # Update minute prices
        service legacy update-prices
    """
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    try:
        from ...services.service_runner import ServiceRunner
        
        runner = ServiceRunner(model_name=model_name)
        
        click.echo("💰 Updating minute prices via pricing service...")
        
        # This leverages the PricingService.update_minute_prices_to_present() method
        results = runner.pricing_service.update_minute_prices_to_present()
        
        click.echo("✅ Price update complete")
        click.echo(f"  • Minute prices created: {results.get('minute_prices_created', 0)}")
        
    except Exception as e:
        raise click.ClickException(f"Price update failed: {e}")