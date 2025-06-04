# indexer/types/__init__.py

from .constants import ZERO_ADDRESS, BURN_ADDRESS, MAX_UINT256

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
from .config import (
    GCSConfig,
    StorageConfig,
    TokenConfig,
    AddressConfig,
    DecoderConfig,
    TransformerConfig,
    ABIConfig,
    ContractConfig,
    DatabaseConfig,
    RpcConfig,
    PathsConfig,
)

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
)

# Model Types: Base
from .model.base import (
    DomainEvent,
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
    AuctionPurchase,
    LotStarted,
    LotCancelled,
)

# Model Types: Farm_logs
from .model.farm_logs import (
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

# Model Types: Fees
from .model.fees import (
    Fee,
)

# Model Types: Liquidity
from .model.liquidity import (
    Position,
    Liquidity,
)

# Model Types: Parameters
from .model.parameters import (
    Parameter,
    ParameterSet,
)

# Model Types: Rewards
from .model.rewards import (
    Reward,
    RewardSet,
)

# Model Types: Staking
from .model.staking import (
    Staking,
)

# Model Types: Trade
from .model.trade import (
    Swap,
    PoolSwap,
    Trade,
)

# Model Types: Transfer
from .model.transfer import (
    Transfer,
    UnmatchedTransfer,
    MatchedTransfer,
    TransferLedger,
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
    "AddressConfig",
    "DecoderConfig",
    "TransformerConfig",
    "ABIConfig",
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
    "DomainEvent",
    "ProcessingError",
    "create_decode_error",
    "create_transform_error",
    "create_rpc_error",
    "AuctionPurchase",
    "LotStarted",
    "LotCancelled",
    "FarmAdd",
    "FarmSet",
    "FarmDeposit",
    "FarmWithdraw",
    "UpdateFarm",
    "FarmHarvest",
    "FarmBatchHarvest",
    "FarmEmergencyWithdraw",
    "FarmSkim",
    "Fee",
    "Position",
    "Liquidity",
    "Parameter",
    "ParameterSet",
    "Reward",
    "RewardSet",
    "Staking",
    "Swap",
    "PoolSwap",
    "Trade",
    "Transfer",
    "UnmatchedTransfer",
    "MatchedTransfer",
    "TransferLedger",
]