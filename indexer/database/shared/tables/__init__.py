# indexer/database/shared/tables/__init__.py

from .config.address import DBAddress
from .config.contract import DBContract
from .config.label import DBLabel
from .config.model_relations import DBModelContract, DBModelToken, DBModelSource
from .config.model import DBModel
from .config.pool import DBPool
from .config.pricing import DBPricing
from .config.source import DBSource
from .config.token import DBToken

from .block_prices import DBBlockPrice
from .periods import DBPeriod
from .price_vwap import DBPriceVwap


__all__ = [
    # Configuration tables
    'DBAddress',
    'DBContract',
    'DBModel',
    'DBPool',
    'DBPricing',
    'DBSource',
    'DBToken',

    # Junction tables
    'DBModelContract',
    'DBModelToken',
    'DBModelSource',

    # Pricing infrastructure
    'DBBlockPrice',
    'DBPeriod',
    'DBPriceVwap'
]