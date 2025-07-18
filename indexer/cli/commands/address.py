# indexer/cli/commands/address.py

"""
Address Management CLI Commands

Fresh design for address configuration management.
"""

import click

@click.group()
def address():
    """Manage addresses and their metadata"""
    pass


@address.command('add')
@click.argument('address')
@click.option('--name', required=True, help='Address name')
@click.option('--type', 'address_type', required=True, 
              type=click.Choice(['wallet', 'router', 'factory', 'treasury', 'multisig', 'other']),
              help='Address type')
@click.option('--project', help='Project name')
@click.option('--description', help='Address description')
@click.option('--grouping', help='Grouping for UI organization')
@click.pass_context
def add(ctx, address, name, address_type, project, description, grouping):
    """Add a new address
    
    Examples:
        # Wallet address
        address add 0x1234... --name "Treasury Wallet" --type wallet --project "BLUB"
        
        # Router address
        address add 0x5678... --name "TraderJoe Router" --type router \\
            --project "TraderJoe" --grouping "DeFi"
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.shared_db_manager.get_session() as session:
            from ...database.shared.tables.config.config import Address
            
            # Check if address already exists
            existing_address = session.query(Address).filter(
                Address.address == address.lower()
            ).first()
            
            if existing_address:
                raise click.ClickException(f"Address '{address}' already exists")
            
            # Create address
            new_address = Address(
                address=address.lower(),
                name=name,
                address_type=address_type,
                project=project,
                description=description,
                grouping=grouping
            )
            
            session.add(new_address)
            session.commit()
            
            click.echo("✅ Address added successfully")
            click.echo(f"   Name: {name}")
            click.echo(f"   Address: {address}")
            click.echo(f"   Type: {address_type}")
            if project:
                click.echo(f"   Project: {project}")
            if grouping:
                click.echo(f"   Grouping: {grouping}")
            
    except Exception as e:
        raise click.ClickException(f"Failed to add address: {e}")


@address.command('list')
@click.option('--type', 'address_type', help='Filter by address type')
@click.option('--project', help='Filter by project')
@click.option('--grouping', help='Filter by grouping')
@click.pass_context
def list_addresses(ctx, address_type, project, grouping):
    """List addresses
    
    Examples:
        # List all addresses
        address list
        
        # List wallet addresses
        address list --type wallet
        
        # List by project
        address list --project "BLUB"
    """
    cli_context = ctx.obj['cli_context']
    
    try:
        with cli_context.shared_db_manager.get_session() as session:
            from ...database.shared.tables.config.config import Address
            
            query = session.query(Address)
            
            if address_type:
                query = query.filter(Address.address_type == address_type)
            
            if project:
                query = query.filter(Address.project == project)
            
            if grouping:
                query = query.filter(Address.grouping == grouping)
            
            addresses = query.order_by(Address.name).all()
            
            if not addresses:
                filters = []
                if address_type:
                    filters.append(f"type '{address_type}'")
                if project:
                    filters.append(f"project '{project}'")
                if grouping:
                    filters.append(f"grouping '{grouping}'")
                
                filter_str = " with " + " and ".join(filters) if filters else ""
                click.echo(f"No addresses found{filter_str}")
                return
            
            click.echo("📍 Addresses")
            click.echo("=" * 80)
            
            type_icons = {
                'wallet': '👛',
                'router': '🔀',
                'factory': '🏭',
                'treasury': '🏦',
                'multisig': '🔐',
                'other': '📍'
            }
            
            # Group by project if no project filter
            if not project:
                projects = {}
                for addr in addresses:
                    proj = addr.project or "No Project"
                    if proj not in projects:
                        projects[proj] = []
                    projects[proj].append(addr)
                
                for proj, proj_addresses in projects.items():
                    click.echo(f"\n🏗️  {proj}")
                    for addr in proj_addresses:
                        icon = type_icons.get(addr.address_type, '📍')
                        click.echo(f"   {icon} {addr.name} ({addr.address_type})")
                        click.echo(f"      Address: {addr.address}")
                        if addr.description:
                            click.echo(f"      Description: {addr.description}")
            else:
                for addr in addresses:
                    icon = type_icons.get(addr.address_type, '📍')
                    click.echo(f"{icon} {addr.name} ({addr.address_type})")
                    click.echo(f"   Address: {addr.address}")
                    if addr.description:
                        click.echo(f"   Description: {addr.description}")
                    click.echo()
            
    except Exception as e:
        raise click.ClickException(f"Failed to list addresses: {e}")

