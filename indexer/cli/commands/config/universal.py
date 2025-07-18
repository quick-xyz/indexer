# indexer/cli/commands/config/universal.py

import click 
import yaml
from pathlib import Path
from typing import Dict, List, Any

from ....database.shared.repositories.config.address_repository import AddressRepository
from ....database.shared.repositories.config.source_repository import SourceRepository
from ....database.shared.repositories.config.model_repository import ModelRepository
from ....database.shared.repositories.config.token_repository import TokenRepository
from ....database.shared.repositories.config.contract_repository import ContractRepository
from ....database.shared.repositories.config.label_repository import LabelRepository
from ....database.shared.repositories.config.pool_repository import PoolRepository
from ....database.shared.repositories.config.pricing_repository import PricingRepository
from ....database.shared.repositories.config.model_relations_repository import ModelRelationsRepository

from ....types.configs.address import AddressConfig
from ....types.configs.source import SourceConfig
from ....types.configs.model import ModelConfig
from ....types.configs.token import TokenConfig
from ....types.configs.contract import ContractConfig
from ....types.configs.label import LabelConfig
from ....types.configs.pool import PoolConfig
from ....types.configs.pricing import PricingConfig
from ....types.configs.model_relations import ModelContractConfig, ModelTokenConfig, ModelSourceConfig


@click.group()
def universal():
    pass


@universal.command('import')
@click.argument('config_file')
@click.option('--dry-run', is_flag=True, help='Preview what would be imported')
@click.option('--stop-on-error', is_flag=True, help='Stop processing on first error')
@click.pass_context
def import_config(ctx, config_file, dry_run, stop_on_error):
    config_path = Path(config_file)
    if not config_path.exists():
        raise click.ClickException(f"Config file not found: {config_file}")
    
    try:
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
    except Exception as e:
        raise click.ClickException(f"Failed to parse YAML: {e}")
    
    analysis = _analyze_config_content(config_data)
    
    if analysis['total_items'] == 0:
        click.echo("No configuration items found in file")
        return
    
    click.echo(f"📋 Universal Configuration Import")
    click.echo("=" * 50)
    click.echo(f"📁 File: {config_file}")
    click.echo(f"📊 Total items: {analysis['total_items']}")
    click.echo(f"🏗️  Sections found: {', '.join(analysis['sections'])}")
    
    if dry_run:
        _preview_universal_import(ctx, config_data, analysis)
    else:
        _execute_universal_import(ctx, config_data, analysis, stop_on_error)


def _analyze_config_content(config_data: Dict[str, Any]) -> Dict[str, Any]:
    sections = {}
    total_items = 0
    
    # Level 1 sections (no dependencies)
    level1_sections = ['addresses', 'sources', 'models']
    
    # Level 2 sections (require addresses)
    level2_sections = ['tokens', 'contracts', 'labels', 'pools']
    
    # Level 3 sections (complex dependencies)
    level3_sections = ['pricing', 'model_contracts', 'model_tokens', 'model_sources']
    
    all_sections = level1_sections + level2_sections + level3_sections
    
    for section in all_sections:
        if section in config_data and config_data[section]:
            count = len(config_data[section])
            sections[section] = count
            total_items += count
    
    return {
        'total_items': total_items,
        'sections': list(sections.keys()),
        'section_counts': sections,
        'level1': {k: v for k, v in sections.items() if k in level1_sections},
        'level2': {k: v for k, v in sections.items() if k in level2_sections},
        'level3': {k: v for k, v in sections.items() if k in level3_sections}
    }


def _preview_universal_import(ctx, config_data: Dict[str, Any], analysis: Dict[str, Any]):
    cli_context = ctx.obj['cli_context']
    
    click.echo(f"\n🔍 DRY RUN - Universal Import Preview")
    click.echo("=" * 60)
    
    repos = _initialize_repositories(cli_context)
    
    total_new = 0
    total_unchanged = 0
    total_errors = 0
    
    for level_name, level_sections in [
        ("Level 1 (Foundation)", analysis['level1']),
        ("Level 2 (Address-dependent)", analysis['level2']),
        ("Level 3 (Complex dependencies)", analysis['level3'])
    ]:
        if not level_sections:
            continue
            
        click.echo(f"\n🏗️  {level_name}:")
        
        for section, count in level_sections.items():
            click.echo(f"\n📋 {section} ({count} items):")
            
            try:
                level_results = _preview_section(repos, section, config_data.get(section, []))
                
                for result in level_results:
                    if result['action'] == 'create':
                        click.echo(f"   ✅ CREATE: {result['message']}")
                        total_new += 1
                    elif result['action'] == 'skip':
                        click.echo(f"   ⏭️  SKIP: {result['message']}")
                        total_unchanged += 1
                    elif result['action'] == 'error':
                        click.echo(f"   ❌ ERROR: {result['message']}")
                        total_errors += 1
                        
            except Exception as e:
                click.echo(f"   ❌ SECTION ERROR: {e}")
                total_errors += count
    
    click.echo(f"\n📊 Universal Preview Summary:")
    click.echo(f"   Total items to create: {total_new}")
    click.echo(f"   Total unchanged items: {total_unchanged}")
    click.echo(f"   Total errors/conflicts: {total_errors}")
    
    if total_errors > 0:
        click.echo(f"\n⚠️  {total_errors} conflicts found. Use update commands to modify existing items.")
    else:
        click.echo(f"\n✅ Ready for universal import!")


def _execute_universal_import(ctx, config_data: Dict[str, Any], analysis: Dict[str, Any], stop_on_error: bool):
    cli_context = ctx.obj['cli_context']
    
    click.echo(f"\n📥 Universal Configuration Import")
    click.echo("=" * 50)
    
    repos = _initialize_repositories(cli_context)
    
    overall_results = {
        'created': [],
        'unchanged': [],
        'errors': [],
        'sections_processed': [],
        'sections_failed': []
    }
    
    levels = [
        ("Level 1 (Foundation)", analysis['level1']),
        ("Level 2 (Address-dependent)", analysis['level2']),
        ("Level 3 (Complex dependencies)", analysis['level3'])
    ]
    
    for level_name, level_sections in levels:
        if not level_sections:
            continue
            
        click.echo(f"\n🏗️  {level_name}:")
        
        for section, count in level_sections.items():
            click.echo(f"\n📋 Processing {section} ({count} items)...")
            
            try:
                section_results = _process_section(repos, section, config_data.get(section, []))
                
                overall_results['created'].extend([f"{section}: {item}" for item in section_results['created']])
                overall_results['unchanged'].extend([f"{section}: {item}" for item in section_results['unchanged']])
                overall_results['errors'].extend(section_results['errors'])
                overall_results['sections_processed'].append(section)
                
                if section_results['created']:
                    click.echo(f"   ✅ Created {len(section_results['created'])} items")
                if section_results['unchanged']:
                    click.echo(f"   ⏭️  Skipped {len(section_results['unchanged'])} unchanged items")
                if section_results['errors']:
                    click.echo(f"   ❌ {len(section_results['errors'])} errors")
                    if stop_on_error:
                        click.echo(f"   🛑 Stopping on error as requested")
                        break
                        
            except Exception as e:
                error_msg = f"Section {section} failed: {e}"
                overall_results['errors'].append(error_msg)
                overall_results['sections_failed'].append(section)
                click.echo(f"   ❌ SECTION FAILED: {e}")
                
                if stop_on_error:
                    click.echo(f"   🛑 Stopping on error as requested")
                    break
        
        if stop_on_error and overall_results['errors']:
            break
    
    _display_final_results(overall_results)


def _initialize_repositories(cli_context):
    return {
        'addresses': AddressRepository(cli_context.shared_db_manager),
        'sources': SourceRepository(cli_context.shared_db_manager),
        'models': ModelRepository(cli_context.shared_db_manager),
        'tokens': TokenRepository(cli_context.shared_db_manager),
        'contracts': ContractRepository(cli_context.shared_db_manager),
        'labels': LabelRepository(cli_context.shared_db_manager),
        'pools': PoolRepository(cli_context.shared_db_manager),
        'pricing': PricingRepository(cli_context.shared_db_manager),
        'relations': ModelRelationsRepository(cli_context.shared_db_manager)
    }


def _preview_section(repos, section: str, data: List[Dict]) -> List[Dict]:
    if section == 'addresses':
        configs = [AddressConfig(**item) for item in data]
        return [repos['addresses'].validate_and_process_config(config) for config in configs]
    elif section == 'sources':
        configs = [SourceConfig(**item) for item in data]
        return [repos['sources'].validate_and_process_config(config) for config in configs]
    elif section == 'models':
        configs = [ModelConfig(**item) for item in data]
        return [repos['models'].validate_and_process_config(config) for config in configs]
    elif section == 'tokens':
        configs = [TokenConfig(**item) for item in data]
        return [repos['tokens'].validate_and_process_config(config) for config in configs]
    elif section == 'contracts':
        configs = [ContractConfig(**item) for item in data]
        return [repos['contracts'].validate_and_process_config(config) for config in configs]
    elif section == 'labels':
        configs = [LabelConfig(**item) for item in data]
        return [repos['labels'].validate_and_process_config(config) for config in configs]
    elif section == 'pools':
        configs = [PoolConfig(**item) for item in data]
        return [repos['pools'].validate_and_process_config(config) for config in configs]
    elif section == 'pricing':
        configs = [PricingConfig(**item) for item in data]
        return [repos['pricing'].validate_and_process_config(config) for config in configs]
    elif section in ['model_contracts', 'model_tokens', 'model_sources']:
        # Handle model relations differently - they're processed as a group
        return [{'action': 'create', 'message': f'Model relation: {item}'} for item in data]
    else:
        raise ValueError(f"Unknown section: {section}")


def _process_section(repos, section: str, data: List[Dict]) -> Dict[str, Any]:
    if section == 'addresses':
        configs = [AddressConfig(**item) for item in data]
        return repos['addresses'].process_configs_batch(configs)
    elif section == 'sources':
        configs = [SourceConfig(**item) for item in data]
        return repos['sources'].process_configs_batch(configs)
    elif section == 'models':
        configs = [ModelConfig(**item) for item in data]
        return repos['models'].process_configs_batch(configs)
    elif section == 'tokens':
        configs = [TokenConfig(**item) for item in data]
        return repos['tokens'].process_configs_batch(configs)
    elif section == 'contracts':
        configs = [ContractConfig(**item) for item in data]
        return repos['contracts'].process_configs_batch(configs)
    elif section == 'labels':
        configs = [LabelConfig(**item) for item in data]
        return repos['labels'].process_configs_batch(configs)
    elif section == 'pools':
        configs = [PoolConfig(**item) for item in data]
        return repos['pools'].process_configs_batch(configs)
    elif section == 'pricing':
        configs = [PricingConfig(**item) for item in data]
        return repos['pricing'].process_configs_batch(configs)
    elif section in ['model_contracts', 'model_tokens', 'model_sources']:
        relations_data = {section: data}
        return repos['relations'].process_relations_batch(relations_data)
    else:
        raise ValueError(f"Unknown section: {section}")


def _display_final_results(results: Dict[str, Any]):
    click.echo(f"\n📊 Universal Import Complete")
    click.echo("=" * 40)
    
    total_created = len(results['created'])
    total_unchanged = len(results['unchanged'])
    total_errors = len(results['errors'])
    sections_processed = len(results['sections_processed'])
    sections_failed = len(results['sections_failed'])
    
    click.echo(f"📈 Summary:")
    click.echo(f"   Sections processed: {sections_processed}")
    click.echo(f"   Sections failed: {sections_failed}")
    click.echo(f"   Items created: {total_created}")
    click.echo(f"   Items unchanged: {total_unchanged}")
    click.echo(f"   Errors: {total_errors}")
    
    if results['created']:
        click.echo(f"\n✅ Created:")
        for item in results['created'][:10]:  # Show first 10
            click.echo(f"   • {item}")
        if len(results['created']) > 10:
            click.echo(f"   ... and {len(results['created']) - 10} more")
    
    if results['unchanged']:
        click.echo(f"\n⏭️ Unchanged:")
        click.echo(f"   {len(results['unchanged'])} items matched existing records")
    
    if results['errors']:
        click.echo(f"\n❌ Errors:")
        for error in results['errors'][:5]:  # Show first 5 errors
            click.echo(f"   • {error}")
        if len(results['errors']) > 5:
            click.echo(f"   ... and {len(results['errors']) - 5} more errors")
    
    if total_errors == 0:
        click.echo(f"\n🎉 Universal import completed successfully!")
        click.echo(f"   Processed {total_created + total_unchanged} items across {sections_processed} sections")
    else:
        click.echo(f"\n⚠️ Universal import completed with {total_errors} errors!")
        click.echo(f"   Successfully processed {total_created + total_unchanged} items")
        if sections_failed:
            click.echo(f"   Failed sections: {', '.join(results['sections_failed'])}")