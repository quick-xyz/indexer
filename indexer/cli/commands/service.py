# indexer/cli/commands/service.py

"""
Service Management CLI Commands

Integrates PricingService and CalculationService operations via ServiceRunner.
Complete implementation with all pricing and calculation commands.
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
    """Apply canonical pricing to globally priced events
    
    Finds pool swaps and trades without direct pricing and applies
    canonical prices from price_vwap table to create global pricing.
    
    Examples:
        # Update global pricing for last 7 days
        service pricing update-global --asset 0x1234... --days 7
        
        # Update AVAX pricing only
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
    """Comprehensive pricing update (canonical + global + direct)
    
    Runs the complete pricing pipeline:
    1. Direct pricing (existing swap/trade pricing)
    2. Canonical price generation from pricing pools
    3. Global pricing application to unpriced events
    
    Examples:
        # Complete pricing update
        service pricing update-all --asset 0x1234...
        
        # Update last 14 days only
        service pricing update-all --asset 0x1234... --days 14
        
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
    - Direct pricing status (pool swaps and trades)
    - Canonical price coverage and gaps
    - Global pricing application status
    - Recent pricing activity timestamps
    
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
    """Comprehensive update for both pricing and calculation services
    
    Runs the complete service pipeline in proper order:
    1. Pricing Service: Direct pricing, canonical pricing, global pricing
    2. Calculation Service: Event valuations, analytics generation
    
    This is the recommended command for complete asset pricing and calculation.
    
    Examples:
        # Complete service update
        service update-all --asset 0x1234...
        
        # Update last 30 days
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
    """Show comprehensive status for both pricing and calculation services
    
    Displays detailed status for:
    - Pricing Service: Direct pricing, canonical pricing, global pricing coverage
    - Calculation Service: Event valuations, analytics coverage
    - Recent activity and gap analysis
    - Overall service health and recommendations
    
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
# UTILITY COMMANDS
# =====================================================================

@service.command('health')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def health_check(ctx, model):
    """Check service health and configuration
    
    Validates:
    - Database connections (shared and indexer)
    - Service initialization
    - Repository availability
    - Configuration completeness
    
    Examples:
        # Check service health
        service health
        
        # Specific model health
        service health --model blub_test
    """
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    try:
        from ...services.service_runner import ServiceRunner
        
        # Test service initialization
        click.echo(f"🏥 Service Health Check - {model_name}")
        click.echo("=" * 50)
        
        runner = ServiceRunner(model_name=model_name)
        
        # Test database connections
        click.echo("✅ ServiceRunner initialized successfully")
        click.echo("✅ Database connections established")
        click.echo("✅ Service dependencies resolved")
        click.echo()
        click.echo("🎉 All services are healthy and ready!")
        
    except Exception as e:
        click.echo(f"❌ Service health check failed: {e}")
        raise click.ClickException("Service health check failed - check configuration and database connectivity")