# indexer/cli/commands/pricing.py

"""
Pricing Service CLI Commands

Fresh design integrating PricingServiceRunner functionality directly into CLI.
Provides all pricing operations with clean command structure.
"""

import click
from typing import Optional, List
from datetime import datetime

@click.group()
def pricing():
    """Pricing service operations and management"""
    pass


@pricing.command('update-periods')
@click.option('--types', help='Comma-separated period types (1min,5min,1hr,4hr,1day)')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def update_periods(ctx, types, model):
    """Update periods to present time
    
    Examples:
        # Update all period types
        pricing update-periods
        
        # Update specific period types
        pricing update-periods --types 5min,1hr
        
        # Update for specific model
        pricing update-periods --model blub_test --types 1min,5min
    """
    # Get model from command option or global context
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    cli_context = ctx.obj['cli_context']
    
    try:
        runner = cli_context.get_pricing_service_runner(model_name)
        
        # Parse period types if provided
        period_types = None
        if types:
            period_types = [t.strip() for t in types.split(',')]
            
            # Validate period types
            valid_types = {'1min', '5min', '1hr', '4hr', '1day'}
            invalid_types = set(period_types) - valid_types
            if invalid_types:
                raise click.BadParameter(f"Invalid period types: {', '.join(invalid_types)}. "
                                       f"Valid types: {', '.join(valid_types)}")
        
        click.echo(f"🕐 Updating periods to present - {model_name}")
        if period_types:
            click.echo(f"📊 Period types: {', '.join(period_types)}")
        else:
            click.echo(f"📊 All period types")
        
        click.echo("=" * 50)
        
        # Execute the update (this calls the existing PricingServiceRunner logic)
        runner.update_periods(period_types)
        
    except Exception as e:
        raise click.ClickException(f"Period update failed: {e}")


@pricing.command('update-prices')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def update_prices(ctx, model):
    """Update minute-by-minute AVAX prices to present
    
    Examples:
        # Update prices for current model
        pricing update-prices
        
        # Update prices for specific model
        pricing update-prices --model blub_test
    """
    # Get model from command option or global context
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    cli_context = ctx.obj['cli_context']
    
    try:
        runner = cli_context.get_pricing_service_runner(model_name)
        
        click.echo(f"💰 Updating minute prices to present - {model_name}")
        click.echo("=" * 50)
        
        # Execute the update
        runner.update_minute_prices()
        
    except Exception as e:
        raise click.ClickException(f"Price update failed: {e}")


@pricing.command('update-swaps')
@click.option('--limit', type=int, default=1000, help='Maximum swaps to process (default: 1000)')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def update_swaps(ctx, limit, model):
    """Update pricing for swaps missing valuation details
    
    Examples:
        # Update swap pricing with default limit
        pricing update-swaps
        
        # Process more swaps in batch
        pricing update-swaps --limit 5000
        
        # Update for specific model
        pricing update-swaps --model blub_test --limit 2000
    """
    # Get model from command option or global context
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    cli_context = ctx.obj['cli_context']
    
    try:
        runner = cli_context.get_pricing_service_runner(model_name)
        
        click.echo(f"🔄 Updating swap pricing - {model_name}")
        click.echo(f"📊 Processing up to {limit:,} swaps")
        click.echo("=" * 50)
        
        # Execute the swap pricing update
        runner.update_swap_pricing(limit=limit)
        
    except Exception as e:
        raise click.ClickException(f"Swap pricing update failed: {e}")


@pricing.command('update-trades')
@click.option('--limit', type=int, default=1000, help='Maximum trades to process (default: 1000)')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def update_trades(ctx, limit, model):
    """Update pricing for trades missing valuation details
    
    Examples:
        # Update trade pricing with default limit
        pricing update-trades
        
        # Process more trades in batch
        pricing update-trades --limit 5000
        
        # Update for specific model
        pricing update-trades --model blub_test --limit 2000
    """
    # Get model from command option or global context
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    cli_context = ctx.obj['cli_context']
    
    try:
        runner = cli_context.get_pricing_service_runner(model_name)
        
        click.echo(f"📈 Updating trade pricing - {model_name}")
        click.echo(f"📊 Processing up to {limit:,} trades")
        click.echo("=" * 50)
        
        # Execute the trade pricing update
        runner.update_trade_pricing(limit=limit)
        
    except Exception as e:
        raise click.ClickException(f"Trade pricing update failed: {e}")


@pricing.command('update-all')
@click.option('--model', help='Model name (overrides global --model option)')
@click.option('--swap-limit', type=int, default=1000, help='Maximum swaps to process (default: 1000)')
@click.option('--trade-limit', type=int, default=1000, help='Maximum trades to process (default: 1000)')
@click.pass_context
def update_all(ctx, model, swap_limit, trade_limit):
    """Update periods, minute prices, swap pricing, AND trade pricing
    
    Examples:
        # Full pricing update (periods + prices + swaps + trades)
        pricing update-all
        
        # Full update with custom limits
        pricing update-all --swap-limit 5000 --trade-limit 2000
        
        # Full update for specific model
        pricing update-all --model blub_test
    """
    # Get model from command option or global context
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    cli_context = ctx.obj['cli_context']
    
    try:
        runner = cli_context.get_pricing_service_runner(model_name)
        
        click.echo(f"🔄 Full pricing update - {model_name}")
        click.echo(f"📊 Processing up to {swap_limit:,} swaps and {trade_limit:,} trades")
        click.echo("=" * 60)
        
        # Execute the full update (includes periods, prices, swaps, and trades)
        runner.update_all_pricing()
        
    except Exception as e:
        raise click.ClickException(f"Full pricing update failed: {e}")


@pricing.command('backfill')
@click.option('--type', 'period_type', required=True,
              type=click.Choice(['1min', '5min', '1hr', '4hr', '1day']),
              help='Period type to backfill')
@click.option('--days', type=int, default=7, help='Days to backfill (default: 7)')
@click.option('--start-date', help='Start date (YYYY-MM-DD)')
@click.option('--end-date', help='End date (YYYY-MM-DD)')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def backfill(ctx, period_type, days, start_date, end_date, model):
    """Backfill missing periods for a specific time range
    
    Examples:
        # Backfill last 7 days of 1hr periods
        pricing backfill --type 1hr --days 7
        
        # Backfill specific date range
        pricing backfill --type 5min --start-date 2024-01-01 --end-date 2024-01-07
        
        # Backfill for specific model
        pricing backfill --type 1hr --days 30 --model blub_test
    """
    # Get model from command option or global context
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    cli_context = ctx.obj['cli_context']
    
    try:
        runner = cli_context.get_pricing_service_runner(model_name)
        
        click.echo(f"📊 Backfilling {period_type} periods - {model_name}")
        if start_date and end_date:
            click.echo(f"📅 Date range: {start_date} to {end_date}")
        else:
            click.echo(f"📅 Last {days} days")
        click.echo("=" * 50)
        
        # Execute the backfill
        runner.backfill_periods(
            period_type=period_type,
            days=days,
            start_date=start_date,
            end_date=end_date
        )
        
    except Exception as e:
        raise click.ClickException(f"Backfill failed: {e}")


@pricing.command('backfill-swaps')
@click.option('--days', type=int, default=7, help='Days back to process swaps (default: 7)')
@click.option('--limit', type=int, default=10000, help='Maximum swaps to process (default: 10000)')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def backfill_swaps(ctx, days, limit, model):
    """Backfill swap pricing for recent swaps
    
    Examples:
        # Backfill last 7 days of swaps
        pricing backfill-swaps
        
        # Backfill last 30 days with higher limit
        pricing backfill-swaps --days 30 --limit 50000
        
        # Backfill for specific model
        pricing backfill-swaps --model blub_test --days 14
    """
    # Get model from command option or global context
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    cli_context = ctx.obj['cli_context']
    
    try:
        runner = cli_context.get_pricing_service_runner(model_name)
        
        click.echo(f"🔄 Backfilling swap pricing - {model_name}")
        click.echo(f"📅 Processing last {days} days")
        click.echo(f"📊 Maximum {limit:,} swaps")
        click.echo("=" * 50)
        
        # For now, just run the regular swap pricing update
        # In the future, this could filter by date range
        runner.update_swap_pricing(limit=limit)
        
        click.echo(f"\n💡 Note: Currently processes most recent {limit:,} swaps.")
        click.echo(f"   Date-based filtering will be added in future updates.")
        
    except Exception as e:
        raise click.ClickException(f"Swap pricing backfill failed: {e}")


@pricing.command('backfill-trades')
@click.option('--days', type=int, default=7, help='Days back to process trades (default: 7)')
@click.option('--limit', type=int, default=10000, help='Maximum trades to process (default: 10000)')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def backfill_trades(ctx, days, limit, model):
    """Backfill trade pricing for recent trades
    
    Examples:
        # Backfill last 7 days of trades
        pricing backfill-trades
        
        # Backfill last 30 days with higher limit
        pricing backfill-trades --days 30 --limit 50000
        
        # Backfill for specific model
        pricing backfill-trades --model blub_test --days 14
    """
    # Get model from command option or global context
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    cli_context = ctx.obj['cli_context']
    
    try:
        runner = cli_context.get_pricing_service_runner(model_name)
        
        click.echo(f"📈 Backfilling trade pricing - {model_name}")
        click.echo(f"📅 Processing last {days} days")
        click.echo(f"📊 Maximum {limit:,} trades")
        click.echo("=" * 50)
        
        # For now, just run the regular trade pricing update
        # In the future, this could filter by date range
        runner.update_trade_pricing(limit=limit)
        
        click.echo(f"\n💡 Note: Currently processes most recent {limit:,} trades.")
        click.echo(f"   Date-based filtering will be added in future updates.")
        
    except Exception as e:
        raise click.ClickException(f"Trade pricing backfill failed: {e}")


@pricing.command('status')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def status(ctx, model):
    """Show comprehensive pricing status including swap and trade pricing
    
    Examples:
        # Show all pricing status
        pricing status
        
        # Show status for specific model
        pricing status --model blub_test
    """
    # Get model from command option or global context
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    cli_context = ctx.obj['cli_context']
    
    try:
        runner = cli_context.get_pricing_service_runner(model_name)
        
        click.echo(f"📊 Comprehensive Pricing Status - {model_name}")
        click.echo("=" * 60)
        
        # Execute the comprehensive status check
        runner.show_pricing_status()
        
    except Exception as e:
        raise click.ClickException(f"Status check failed: {e}")


@pricing.command('validate')
@click.option('--sample-size', type=int, default=1000, help='Number of samples to validate (default: 1000)')
@click.option('--model', help='Model name (overrides global --model option)')
@click.pass_context
def validate(ctx, sample_size, model):
    """Validate pricing accuracy including swap and trade pricing data quality
    
    Examples:
        # Basic validation
        pricing validate
        
        # Validate with larger sample
        pricing validate --sample-size 5000
        
        # Validate specific model
        pricing validate --model blub_test --sample-size 2000
    """
    # Get model from command option or global context
    model_name = model or ctx.obj.get('model')
    if not model_name:
        raise click.ClickException("Model name required. Use --model option or global --model flag")
    
    cli_context = ctx.obj['cli_context']
    
    try:
        runner = cli_context.get_pricing_service_runner(model_name)
        
        click.echo(f"🔍 Comprehensive Pricing Validation - {model_name}")
        click.echo(f"📊 Sample size: {sample_size:,}")
        click.echo("=" * 50)
        
        # Execute comprehensive validation (includes swap and trade pricing)
        runner.validate_swap_pricing(sample_size=sample_size)
        
    except Exception as e:
        raise click.ClickException(f"Validation failed: {e}")