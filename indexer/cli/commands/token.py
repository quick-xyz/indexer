# indexer/cli/commands/token.py

"""
Token Management CLI Commands

Fresh design for token configuration management.
"""

import click
from sqlalchemy import and_

@click.group()
def token():
    """Manage global token metadata"""
    pass


@token.command('create')
@click.argument('address')
@click.option('--symbol', required=True, help='Token symbol')
@click.option('--name', required=True, help='Token name')
@click.option('--decimals', type=int, required=True, help='Token decimals')
@click.option('--project', help='Project name')
@click.option('--type', 'token_type', default='token', 
              type=click.Choice(['token', 'lp_receipt', 'nft']),
              help='Token type (default: token)')
@click.option('--description', help='Token description')
@click.pass_context
def create(ctx, address, symbol, name, decimals, project, token_type, description):
    """Create global token metadata
    
    Examples:
        # Basic token
        token create 0x1234... --symbol BLUB --name "BLUB Token" --decimals 18
        
        # LP receipt token
        token create 0x5678... --symbol BLUB-AVAX-LP --name "BLUB-AVAX LP" \\
            --decimals 18 --type lp_receipt --project "TraderJoe"
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config.config import Token
            
            # Check if token already exists
            existing_token = session.query(Token).filter(
                Token.address == address.lower()
            ).first()
            
            if existing_token:
                raise click.ClickException(f"Token '{address}' already exists")
            
            # Create token
            new_token = Token(
                address=address.lower(),
                symbol=symbol,
                name=name,
                decimals=decimals,
                project=project,
                token_type=token_type,
                description=description
            )
            
            session.add(new_token)
            session.commit()
            
            click.echo("✅ Token created successfully")
            click.echo(f"   Symbol: {symbol}")
            click.echo(f"   Name: {name}")
            click.echo(f"   Address: {address}")
            click.echo(f"   Decimals: {decimals}")
            click.echo(f"   Type: {token_type}")
            if project:
                click.echo(f"   Project: {project}")
            
    except Exception as e:
        raise click.ClickException(f"Failed to create token: {e}")


@token.command('list')
@click.option('--project', help='Filter by project')
@click.option('--type', 'token_type', help='Filter by token type')
@click.pass_context
def list_tokens(ctx, project, token_type):
    """List all tokens
    
    Examples:
        # List all tokens
        token list
        
        # List tokens by project
        token list --project "TraderJoe"
        
        # List LP tokens
        token list --type lp_receipt
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.infrastructure_db_manager.get_session() as session:
            from ...database.shared.tables.config.config import Token
            
            query = session.query(Token)
            
            if project:
                query = query.filter(Token.project == project)
            
            if token_type:
                query = query.filter(Token.token_type == token_type)
            
            tokens = query.order_by(Token.symbol).all()
            
            if not tokens:
                filters = []
                if project:
                    filters.append(f"project '{project}'")
                if token_type:
                    filters.append(f"type '{token_type}'")
                
                filter_str = " with " + " and ".join(filters) if filters else ""
                click.echo(f"No tokens found{filter_str}")
                return
            
            click.echo("🪙 Tokens")
            click.echo("=" * 80)
            
            type_icons = {
                'token': '🪙',
                'lp_receipt': '🎫',
                'nft': '🖼️'
            }
            
            for token in tokens:
                icon = type_icons.get(token.token_type, '🪙')
                click.echo(f"{icon} {token.symbol} ({token.name})")
                click.echo(f"   Address: {token.address}")
                click.echo(f"   Decimals: {token.decimals}")
                click.echo(f"   Type: {token.token_type}")
                if token.project:
                    click.echo(f"   Project: {token.project}")
                if token.description:
                    click.echo(f"   Description: {token.description}")
                click.echo()
            
    except Exception as e:
        raise click.ClickException(f"Failed to list tokens: {e}")