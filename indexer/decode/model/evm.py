from msgspec import Struct,field
from typing import Optional, Any

from .types import HexStr, HexInt, EvmAddress, EvmHash


class EvmLog(Struct):
    address: EvmAddress
    blockHash: EvmHash
    blockNumber: HexStr
    data: HexStr
    logIndex: HexInt
    removed: bool # False, when it hasn't been removed during reorg
    topics: list[EvmHash]
    transactionHash: EvmHash
    transactionIndex: HexStr


class EvmTxReceipt(Struct):
    blockHash: EvmHash
    blockNumber: HexInt
    contractAddress: Optional[EvmAddress]
    cumulativeGasUsed: HexStr
    effectiveGasPrice: HexStr
    from_: EvmAddress = field(name="from")  # from is protected word in python
    gasUsed: HexStr
    logs: list[EvmLog]
    logsBloom: Any
    status: HexInt # 1 (Success) or 2 (Failure)
    to: Optional[EvmAddress]
    transactionHash: EvmHash
    transactionIndex: HexInt
    type: HexStr


class EvmTransaction(Struct):
    accessList: Optional[list[Any]]
    blockHash: EvmHash
    blockNumber: HexInt
    chainId: Optional[HexInt]
    from_: EvmAddress = field(name="from")  # from is protected word in python
    gas: HexStr
    gasPrice: HexStr
    hash: EvmHash
    input: HexStr
    maxFeePerGas: HexStr
    maxPriorityFeePerGas: HexStr
    nonce: HexInt
    r: EvmHash
    s: EvmHash
    to: Optional[EvmAddress]
    transactionIndex: HexInt
    type: HexInt
    v: HexInt
    value: HexInt

class EvmFilteredBlock(Struct):
    block: HexStr
    timestamp: HexInt # unix timestamp in hexadecimal
    transactions: list[Optional[EvmTransaction]]
    receipts: list[Optional[EvmTransaction]]