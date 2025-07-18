# indexer/types/__init__.py

from .constants import ZERO_ADDRESS, BURN_ADDRESS, MAX_UINT256
from typing import Union

# New Types
from .new import (
    BlockID,
    DateTimeStr,
    IntStr,
    BaseStr,
    HexStr,
    HexInt,
    EvmHash,
    HexAddress,
    EvmAddress,
    ChecksumAddress,
    TxEventId,
    DomainEventId,
    ErrorId,
)

# EVM Types
from .evm import (
    EvmLog,
    EvmTxReceipt,
    EvmTransaction,
    EvmFilteredBlock,
)

# Configuration Types
from .configs.config import (
    GCSConfig,
    StorageConfig,
    DatabaseConfig,
    RpcConfig,
    PathsConfig,
)

from .configs.token import TokenConfig
from .configs.pool import PoolConfig
from .configs.address import AddressConfig 
from .configs.label import LabelConfig
from .configs.source    import SourceConfig
from .configs.model import ModelConfig
from .configs.contract import ContractConfig
from .configs.pricing import PricingConfig

# Indexer Types
from .indexer import (
    BlockStatus,
    TransactionStatus,
    ProcessingMetadata,
    EncodedLog,
    DecodedLog,
    DecodedMethod,
    EncodedMethod,
    Transaction,
    Block,
    DomainEventUnion,
    SignalUnion,
)

# Model Types: Base
from .model.base import (
    DomainEvent,
    Signal,
)

# Model Types: Errors
from .model.errors import (
    ProcessingError,
    create_decode_error,
    create_transform_error,
    create_rpc_error,
)

# Model Types: Auction
from .model.auction import (
    AuctionPurchaseSignal,
    LotStartSignal,
    LotCancelSignal,
    AuctionPurchase,
    LotStarted,
    LotCancelled,
)

# Model Types: Farm_logs
from .model.farm import (
    FarmAddSignal,
    FarmSetSignal,
    FarmDepositSignal,
    FarmWithdrawSignal,
    UpdateFarmSignal,
    FarmHarvestSignal,
    FarmBatchHarvestSignal,
    FarmEmergencyWithdrawSignal,
    FarmSkimSignal,
    FarmAdd,
    FarmSet,
    FarmDeposit,
    FarmWithdraw,
    UpdateFarm,
    FarmHarvest,
    FarmBatchHarvest,
    FarmEmergencyWithdraw,
    FarmSkim,
)

# Model Types: Liquidity
from .model.liquidity import (
    LiquiditySignal,
    Liquidity,
)

# Model Types: NFP
from .model.nfp import (
    NfpCollectSignal,
    NfpLiquiditySignal,
)

# Model Types: Parameters
from .model.parameters import (
    ParameterSignal,
    ParameterChange,
    ParameterSetChange,
)

# Model Types: Postions
from .model.positions import (
    Position,
)

# Model Types: Rewards
from .model.rewards import (
    CollectSignal,
    RewardSignal,
    Reward,
)

# Model Types: Staking
from .model.staking import (
    Staking,
)

# Model Types: Trade
from .model.trade import (
    SwapSignal,
    SwapBatchSignal,
    RouteSignal,
    MultiRouteSignal,
    PoolSwap,
    Trade,
)

# Model Types: Transfer
from .model.transfer import (
    TransferSignal,
    Transfer,
    UnknownTransfer,
)

__all__ = [
    # Constants
    "ZERO_ADDRESS",
    "BURN_ADDRESS", 
    "MAX_UINT256",

    # New Types
    "BlockID",
    "DateTimeStr",
    "IntStr",
    "BaseStr",
    "HexStr",
    "HexInt",
    "EvmHash",
    "HexAddress",
    "EvmAddress",
    "ChecksumAddress",
    "TxEventId",
    "DomainEventId",  
    "ErrorId",

    # EVM types
    "EvmLog",
    "EvmTxReceipt",
    "EvmTransaction",
    "EvmFilteredBlock",
    
    # Configuration types
    "GCSConfig",
    "StorageConfig",
    "TokenConfig",
    "PoolConfig",
    "AddressConfig",
    "LabelConfig",
    "SourceConfig",
    "PricingConfig",
    "ModelConfig",
    "DecoderConfig",
    "TransformerConfig",
    "ContractConfig",
    "DatabaseConfig",
    "RpcConfig",
    "PathsConfig",
    
    # Indexer Types
    "BlockStatus",
    "TransactionStatus",
    "ProcessingMetadata",
    "EncodedLog",
    "DecodedLog",
    "DecodedMethod",
    "EncodedMethod",
    "Transaction",
    "Block",

    # Model Types
    ## Base
    "DomainEvent",
    "Signal",
    ## Errors
    "ProcessingError",
    "create_decode_error",
    "create_transform_error",
    "create_rpc_error",
    ## Auction
    "AuctionPurchaseSignal",
    "LotStartSignal",
    "LotCancelSignal",
    "AuctionPurchase",
    "LotStarted",
    "LotCancelled",
    ## Farm
    "FarmAddSignal",
    "FarmSetSignal",
    "FarmDepositSignal",
    "FarmWithdrawSignal",
    "UpdateFarmSignal",
    "FarmHarvestSignal",
    "FarmBatchHarvestSignal",
    "FarmEmergencyWithdrawSignal",
    "FarmSkimSignal",
    "FarmAdd",
    "FarmSet",
    "FarmDeposit",
    "FarmWithdraw",
    "UpdateFarm",
    "FarmHarvest",
    "FarmBatchHarvest",
    "FarmEmergencyWithdraw",
    "FarmSkim",
    ## Liquidity
    "LiquiditySignal",
    "Liquidity",
    ## Nfp
    "NfpCollectSignal",
    "NfpLiquiditySignal",
    ## Parameters
    "ParameterSignal",
    "ParameterChange",
    "ParameterSetChange",
    ## Positions
    "Position",
    ## Rewards
    "CollectSignal",
    "RewardSignal",
    "Reward",
    ## Staking
    "Staking",
    ## Trade
    "SwapSignal",
    "SwapBatchSignal",
    "RouteSignal",
    "MultiRouteSignal",
    "PoolSwap",
    "Trade",
    ## Transfer
    "TransferSignal",
    "Transfer",
    "UnknownTransfer",
    # Type Unions
    "DomainEventUnion",
    "SignalUnion",
]