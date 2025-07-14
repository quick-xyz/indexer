# indexer/types/indexer.py

from typing import Optional, Dict, Literal, Union
from msgspec import Struct

from .new import HexStr,EvmAddress,EvmHash, DomainEventId, ErrorId
from .model.errors import ProcessingError
from .model.base import DomainEvent, Signal
from .model.positions import Position


# Import all possible domain event types for the union
from .model.trade import Trade, PoolSwap, SwapSignal, SwapBatchSignal, RouteSignal, MultiRouteSignal
from .model.transfer import Transfer, UnknownTransfer, TransferSignal
from .model.liquidity import Liquidity, LiquiditySignal
from .model.rewards import Reward, CollectSignal, RewardSignal
from .model.auction import AuctionPurchase, LotStarted, LotCancelled, AuctionPurchaseSignal, LotStartSignal, LotCancelSignal
from .model.farm import (
    FarmAdd, FarmSet, FarmDeposit, FarmWithdraw, 
    UpdateFarm, FarmHarvest, FarmBatchHarvest, 
    FarmEmergencyWithdraw, FarmSkim,
    FarmAddSignal, FarmSetSignal, FarmDepositSignal, FarmWithdrawSignal,
    UpdateFarmSignal, FarmHarvestSignal, FarmBatchHarvestSignal,
    FarmEmergencyWithdrawSignal, FarmSkimSignal
)
from .model.parameters import ParameterChange, ParameterSetChange, ParameterSignal

# Create union of all possible domain events
# Note: All domain event types already have tag=True
DomainEventUnion = Union[
    # Trading events
    Trade,
    PoolSwap,
    
    # Transfer events  
    Transfer,
    UnknownTransfer,
    
    # Other domain events
    Liquidity,
    Reward,
    Position,  # Position is also a domain event
    
    # Auction events
    AuctionPurchase,
    LotStarted, 
    LotCancelled,
    
    # Farm events
    FarmAdd,
    FarmSet,
    FarmDeposit,
    FarmWithdraw,
    UpdateFarm,
    FarmHarvest,
    FarmBatchHarvest,
    FarmEmergencyWithdraw,
    FarmSkim,
    
    # Parameter events
    ParameterChange,
    ParameterSetChange,
]

# For signals, we need to exclude the base Signal class since it's not tagged
# Only include the tagged signal subclasses
SignalUnion = Union[
    # Trading signals
    SwapSignal,
    SwapBatchSignal, 
    RouteSignal,
    MultiRouteSignal,
    
    # Transfer signals
    TransferSignal,
    
    # Other signals
    LiquiditySignal,
    CollectSignal,
    RewardSignal,
    
    # Auction signals
    AuctionPurchaseSignal,
    LotStartSignal,
    LotCancelSignal,
    
    # Farm signals
    FarmAddSignal,
    FarmSetSignal,
    FarmDepositSignal,
    FarmWithdrawSignal,
    UpdateFarmSignal,
    FarmHarvestSignal,
    FarmBatchHarvestSignal,
    FarmEmergencyWithdrawSignal,
    FarmSkimSignal,
    
    # Parameter signals
    ParameterSignal,
]

BlockStatus = Literal["rpc", "processing", "complete", "error"]
TransactionStatus = Literal["decoded", "transformed", "error"]


class ProcessingMetadata(Struct, kw_only=True):
    error_count: int = 0
    retry_count: int = 0
    last_error: Optional[str] = None
    started_at: Optional[str] = None  # ISO timestamp
    completed_at: Optional[str] = None
    error_stage: Optional[str] = None  # "decode", "transform"

class EncodedLog(Struct, tag=True):
    index: int
    removed: bool
    contract: EvmAddress
    signature: EvmHash
    topics: list[EvmHash]
    data: HexStr

class DecodedLog(Struct, tag=True):
    index: int
    removed: bool
    contract: EvmAddress
    signature: EvmHash
    name: str
    attributes: dict

class DecodedMethod(Struct, tag=True):
    selector: Optional[HexStr] = None
    name: Optional[str] = None
    args: Optional[dict] = None

class EncodedMethod(Struct, tag=True):
    data: HexStr

class Transaction(Struct):
    block: int
    timestamp: int
    tx_hash: EvmHash
    index: int
    origin_from: EvmAddress
    function: EncodedMethod | DecodedMethod
    value: str  # Changed from int to str
    tx_success: bool
    logs: Dict[int,EncodedLog|DecodedLog]  # keyed by log index
    origin_to: Optional[EvmAddress] = None
    signals: Optional[Dict[int,SignalUnion]] = None
    events: Optional[Dict[DomainEventId,DomainEventUnion]] = None
    positions: Optional[Dict[DomainEventId,Position]] = None
    errors: Optional[Dict[ErrorId,ProcessingError]] = None
    indexing_status: Optional[TransactionStatus] = None
    processing_metadata: Optional[ProcessingMetadata] = None

class Block(Struct):
    block_number: int
    timestamp: int
    transactions: Optional[Dict[EvmHash,Transaction]] = None # keyed by transaction hash
    indexing_status: Optional[str] = None
    processing_metadata: Optional[ProcessingMetadata] = None