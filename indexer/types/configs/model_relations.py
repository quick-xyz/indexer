# indexer/types/configs/model_relations.py

from msgspec import Struct

from ..new import EvmAddress


class ModelContractConfig(Struct):
    model: str
    contract_address: EvmAddress
    status: str = 'active'


class ModelTokenConfig(Struct):
    model: str
    token_address: EvmAddress
    status: str = 'active'


class ModelSourceConfig(Struct):
    model: str
    source_name: str
    status: str = 'active'