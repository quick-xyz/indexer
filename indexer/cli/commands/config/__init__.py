# indexer/cli/commands/config/__init__.py

import click

@click.group()
def config():
    pass

from .universal import universal
from .addresses import addresses
from .sources import sources
from .models import models
from .tokens import tokens
from .contracts import contracts
from .labels import labels
from .pools import pools
from .pricing import pricing
from .model_relations import model_relations

config.add_command(universal)

config.add_command(addresses)
config.add_command(sources)
config.add_command(models)
config.add_command(tokens)
config.add_command(contracts)
config.add_command(labels)
config.add_command(pools)
config.add_command(pricing)
config.add_command(model_relations)